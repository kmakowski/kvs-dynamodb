package kmakowski.kvs.dynamodb

import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import java.net.URI
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DynamoDbKeyValueStoreTest {
    companion object {
        private const val TABLE = "kvs-test"

        @Container
        @JvmStatic
        val dynamodb: GenericContainer<*> = GenericContainer(DockerImageName.parse("amazon/dynamodb-local:2.6.0"))
            .withExposedPorts(8000)
            .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb", "-port", "8000")

        private val client: DynamoDbAsyncClient by lazy {
            DynamoDbAsyncClient.builder()
                .endpointOverride(URI.create("http://${dynamodb.host}:${dynamodb.getMappedPort(8000)}"))
                .region(Region.US_EAST_1)
                .credentialsProvider(
                    StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"))
                )
                .build()
        }
    }

    private val fixedInstant: Instant = Instant.parse("2026-01-02T03:04:05Z")
    private val clock: Clock = Clock.fixed(fixedInstant, ZoneOffset.UTC)

    @BeforeAll
    fun createTable() {
        runBlocking {
            createTableIfNeeded(DynamoDbConfig(client, TABLE))
        }
    }

    @Test
    fun `put and get string`() {
        runBlocking {
            val store = store("put-get")

            assertThat(store.getObjectAsString("krz")).isNull()
            assertThat(store.exists("krz")).isFalse()

            store.putStringValue("krz", "makowski")

            assertThat(store.getObjectAsString("krz")).isEqualTo("makowski")
            assertThat(store.exists("krz")).isTrue()
            assertThat(store.getObjectAsStringOrThrow("krz")).isEqualTo("makowski")

            store.putStringValue("krz", "updated")
            assertThat(store.getObjectAsString("krz")).isEqualTo("updated")
        }
    }

    @Test
    fun `round trips json objects`() {
        runBlocking {
            val store = store("json")
            store.putObjectAsJson("someKey2", SomeClass("value1", "value2"))

            val result = store.getObject("someKey2", SomeClass::class.java)

            assertThat(result).isEqualTo(SomeClass("value1", "value2"))
            assertThat(store.listObjects("someKey2").single().timestamp).isEqualTo(fixedInstant)
        }
    }

    @Test
    fun `rejects blank keys on put and get`() {
        val store = store("blank")
        assertThrows<IllegalArgumentException> {
            runBlocking { store.putStringValue("  ", "value") }
        }
        assertThrows<IllegalArgumentException> {
            runBlocking { store.getObjectAsString("") }
        }
        assertThrows<IllegalArgumentException> {
            runBlocking { store.getObjectAsStringOrThrow("missing") }
        }
    }

    @Test
    fun `lists by prefix in sort key order`() {
        runBlocking {
            val store = store("list", pageSize = 2)
            listOf("b", "a", "c", "a/child", "ab").forEach { store.putStringValue(it, "v-$it") }

            assertThat(store.listKeys("")).containsExactly("a", "a/child", "ab", "b", "c")
            assertThat(store.listKeys("a")).containsExactly("a", "a/child", "ab")

            val page = store.listObjects(listPrefix = "", maxItems = 2)
            assertThat(page.map { it.key }).containsExactly("a", "a/child")
            assertThat(page.map { it.value }).containsExactly("v-a", "v-a/child")
            assertThat(page.map { it.timestamp }).containsOnly(fixedInstant)

            assertThat(store.listObjects(startAfterKey = "a").map { it.key })
                .containsExactly("a/child", "ab", "b", "c")
            assertThat(store.listObjects(startAfterKey = "aa").map { it.key })
                .containsExactly("ab", "b", "c")
            assertThat(store.listObjects(startAfterKey = "ab").map { it.key })
                .containsExactly("b", "c")
            assertThat(store.listObjects(listPrefix = "a", startAfterKey = "m")).isEmpty()
            assertThat(store.listObjects(listPrefix = "b", startAfterKey = "a").map { it.key })
                .containsExactly("b")
        }
    }

    @Test
    fun `deleteObject removes keys that start with the given key`() {
        runBlocking {
            val store = store("delete", pageSize = 2)
            listOf("foo", "foo/bar", "foobar", "qux").forEach { store.putStringValue(it, it) }

            store.deleteObject("foo")

            assertThat(store.listKeys("")).containsExactly("qux")
            assertThat(store.getObjectAsString("foo")).isNull()
            assertThat(store.getObjectAsString("foobar")).isNull()
        }
    }

    @Test
    fun `deleteObject with an empty key removes the partition`() {
        runBlocking {
            val store = store("wipe")
            store.putStringValue("a", "1")
            store.putStringValue("b", "2")

            store.deleteObject("")

            assertThat(store.listKeys("")).isEmpty()
        }
    }

    @Test
    fun `partitions are isolated`() {
        runBlocking {
            val first = store("partition-a")
            val second = store("partition-b")
            first.putStringValue("k", "from-a")
            second.putStringValue("k", "from-b")

            assertThat(first.getObjectAsString("k")).isEqualTo("from-a")
            assertThat(second.getObjectAsString("k")).isEqualTo("from-b")
            assertThat(first.listKeys("")).containsExactly("k")
        }
    }

    @Test
    fun `list helpers use the interface encoding`() {
        runBlocking {
            val store = store("lists")
            store.addListItem("someKey", "ids", "item_1")
            store.addListItem("someKey", "ids", "item_2")
            store.addListItem("someKey", "ids", "item_3")
            store.deleteListItem("someKey", "ids", "item_2")

            assertThat(store.getListItems("someKey", "ids")).containsExactly("item_1", "item_3")
        }
    }

    private fun store(partition: String, pageSize: Int = 1000) = DynamoDbKeyValueStore(
        DynamoDbConfig(client, TABLE, partition),
        clock = clock,
        pageSize = pageSize,
    )

    data class SomeClass(val key1: String, val key2: String)
}
