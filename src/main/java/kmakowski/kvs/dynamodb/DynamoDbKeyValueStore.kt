package kmakowski.kvs.dynamodb

import io.github.kmakowski.json.Json
import kmakowski.kvs.KeyValue
import kmakowski.kvs.KeyValueStore
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.BillingMode
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import software.amazon.awssdk.services.dynamodb.model.TableStatus
import software.amazon.awssdk.services.dynamodb.model.WriteRequest
import java.time.Clock
import java.time.Instant
import java.util.concurrent.CompletionException
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

private const val PK = "pk"
private const val SK = "sk"
private const val VALUE = "value"
private const val UPDATED_AT = "updatedAt"

data class DynamoDbConfig(
    val client: DynamoDbAsyncClient,
    val tableName: String,
    val partition: String = "kv",
)

class DynamoDbKeyValueStore(
    private val dynamo: DynamoDbConfig,
    private val clock: Clock = Clock.systemUTC(),
    private val consistentRead: Boolean = true,
    private val pageSize: Int = 1000,
) : KeyValueStore {
    private val log = LoggerFactory.getLogger(DynamoDbKeyValueStore::class.java)

    init {
        require(pageSize > 0) { "pageSize must be positive" }
        require(dynamo.tableName.isNotBlank()) { "tableName must not be blank" }
        require(dynamo.partition.isNotBlank()) { "partition must not be blank" }
    }

    override suspend fun putObjectAsJson(key: String, any: Any) {
        log.debug("putObjectAsJson: key={}", key)
        putStringValue(key, Json.serialize(any))
    }

    override suspend fun putStringValue(key: String, value: String) {
        requireKey(key)
        log.debug("putStringValue: key={}", key)
        dynamo.client.putItem { request ->
            request.tableName(dynamo.tableName)
                .item(
                    mapOf(
                        PK to AttributeValue.fromS(dynamo.partition),
                        SK to AttributeValue.fromS(key),
                        VALUE to AttributeValue.fromS(value),
                        UPDATED_AT to AttributeValue.fromS(clock.instant().toString()),
                    )
                )
        }.await()
    }

    override suspend fun <T> getObject(key: String, clazz: Class<T>): T? =
        getObjectAsString(key)?.let { Json.deserialize(it, clazz) }

    override suspend fun getObjectAsString(key: String): String? {
        requireKey(key)
        log.debug("getObjectAsString: key={}", key)
        val response = dynamo.client.getItem { request ->
            request.tableName(dynamo.tableName)
                .key(itemKey(key))
                .consistentRead(consistentRead)
        }.await()
        if (!response.hasItem()) return null
        return response.item()[VALUE]?.s()
    }

    override suspend fun listObjects(
        listPrefix: String,
        startAfterKey: String?,
        maxItems: Int,
    ): List<KeyValue> {
        log.debug("listObjects: prefix={}, startAfter={}, maxItems={}", listPrefix, startAfterKey, maxItems)
        return queryItems(listPrefix, startAfterKey, maxItems, keysOnly = false).map { item ->
            val key = item.getValue(SK).s()
            val value = item[VALUE]?.s()
                ?: throw IllegalStateException("Missing value for key $key")
            val timestamp = item[UPDATED_AT]?.s()?.let(Instant::parse)
                ?: throw IllegalStateException("Missing updatedAt for key $key")
            KeyValue(key, value, timestamp)
        }
    }

    override suspend fun listKeys(listPrefix: String): List<String> {
        log.debug("listKeys: prefix={}", listPrefix)
        return queryItems(listPrefix, startAfterKey = null, maxItems = null, keysOnly = true)
            .map { it.getValue(SK).s() }
    }

    override suspend fun deleteObject(key: String) {
        val keys = listKeys(key)
        if (keys.isEmpty()) {
            log.debug("deleteObject: nothing to delete for key={}", key)
            return
        }
        log.debug("deleteObject: deleting {} items for key={}", keys.size, key)
        keys.chunked(25).forEach { chunk -> deleteChunk(chunk) }
    }

    private suspend fun deleteChunk(keys: List<String>) {
        var pending = keys.map { deleteRequest(it) }
        var attempt = 0
        while (pending.isNotEmpty()) {
            val response = dynamo.client.batchWriteItem { request ->
                request.requestItems(mapOf(dynamo.tableName to pending))
            }.await()
            pending = response.unprocessedItems()[dynamo.tableName].orEmpty()
            if (pending.isEmpty()) return
            attempt += 1
            if (attempt >= 8) {
                throw IllegalStateException("Failed to delete ${pending.size} items from ${dynamo.tableName}")
            }
            delay(50L * attempt)
        }
    }

    private fun deleteRequest(key: String): WriteRequest =
        WriteRequest.builder()
            .deleteRequest(DeleteRequest.builder().key(itemKey(key)).build())
            .build()

    private suspend fun queryItems(
        listPrefix: String,
        startAfterKey: String?,
        maxItems: Int?,
        keysOnly: Boolean,
    ): List<Map<String, AttributeValue>> {
        if (maxItems != null && maxItems <= 0) return emptyList()
        var exclusiveStart = when (val start = pageStart(listPrefix, startAfterKey)) {
            PageStart.None -> return emptyList()
            PageStart.Beginning -> null
            is PageStart.After -> start.key
        }
        val collected = mutableListOf<Map<String, AttributeValue>>()
        while (maxItems == null || collected.size < maxItems) {
            val limit = if (maxItems == null) pageSize else minOf(pageSize, maxItems - collected.size)
            val response = dynamo.client.query { request ->
                request.tableName(dynamo.tableName)
                    .consistentRead(consistentRead)
                    .keyConditionExpression(keyCondition(listPrefix))
                    .expressionAttributeValues(attributeValues(listPrefix))
                    .limit(limit)
                if (keysOnly) {
                    request.projectionExpression("#sk")
                        .expressionAttributeNames(mapOf("#sk" to SK))
                }
                if (exclusiveStart != null) {
                    request.exclusiveStartKey(exclusiveStart)
                }
            }.await()
            collected += response.items().orEmpty()
            val lastKey = response.lastEvaluatedKey()
            if (lastKey.isNullOrEmpty()) break
            exclusiveStart = lastKey
        }
        return if (maxItems == null) collected else collected.take(maxItems)
    }

    private fun pageStart(listPrefix: String, startAfterKey: String?): PageStart {
        if (startAfterKey == null) return PageStart.Beginning
        if (listPrefix.isNotEmpty() && !startAfterKey.startsWith(listPrefix)) {
            return if (startAfterKey < listPrefix) PageStart.Beginning else PageStart.None
        }
        return PageStart.After(itemKey(startAfterKey))
    }

    private fun keyCondition(listPrefix: String): String =
        if (listPrefix.isEmpty()) "$PK = :pk" else "$PK = :pk AND begins_with($SK, :prefix)"

    private fun attributeValues(listPrefix: String): Map<String, AttributeValue> {
        val values = mutableMapOf(":pk" to AttributeValue.fromS(dynamo.partition))
        if (listPrefix.isNotEmpty()) {
            values[":prefix"] = AttributeValue.fromS(listPrefix)
        }
        return values
    }

    private fun itemKey(key: String): Map<String, AttributeValue> = mapOf(
        PK to AttributeValue.fromS(dynamo.partition),
        SK to AttributeValue.fromS(key),
    )

    private fun requireKey(key: String) {
        require(key.isNotBlank()) { "Key must not be blank" }
    }
}

private sealed interface PageStart {
    data object Beginning : PageStart
    data object None : PageStart
    data class After(val key: Map<String, AttributeValue>) : PageStart
}

suspend fun createTableIfNeeded(config: DynamoDbConfig) {
    require(config.tableName.isNotBlank()) { "tableName must not be blank" }
    try {
        config.client.describeTable { it.tableName(config.tableName) }.await()
    } catch (error: Exception) {
        if (unwrap(error) !is ResourceNotFoundException) throw error
        try {
            config.client.createTable { request ->
                request.tableName(config.tableName)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(
                        AttributeDefinition.builder().attributeName(PK).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(SK).attributeType(ScalarAttributeType.S).build(),
                    )
                    .keySchema(
                        KeySchemaElement.builder().attributeName(PK).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(SK).keyType(KeyType.RANGE).build(),
                    )
            }.await()
        } catch (createError: Exception) {
            if (unwrap(createError) !is ResourceInUseException) throw createError
        }
    }
    waitUntilActive(config)
}

private suspend fun waitUntilActive(config: DynamoDbConfig) {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60)
    while (true) {
        val status = config.client.describeTable { it.tableName(config.tableName) }
            .await()
            .table()
            .tableStatus()
        if (status == TableStatus.ACTIVE) return
        if (System.nanoTime() > deadline) {
            throw IllegalStateException("Timed out waiting for table ${config.tableName} to become ACTIVE (status $status)")
        }
        delay(200)
    }
}

private tailrec fun unwrap(error: Throwable): Throwable {
    val cause = error.cause
    if (cause != null && (error is CompletionException || error is ExecutionException)) {
        return unwrap(cause)
    }
    return error
}
