package kmakowski.kvs.dynamodb

import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import java.net.URI
import java.util.UUID

/**
 * On-demand check against a real table. Not part of `mvn test`.
 *
 * Set `DYNAMODB_TABLE`. Optional: `AWS_REGION`, `DYNAMODB_ENDPOINT`.
 */
class DynamoDbKeyValueStoreIT {
    @Test
    fun `writes and reads a configured table`() {
        val table = System.getenv("DYNAMODB_TABLE")
            ?: error("Set DYNAMODB_TABLE to the table this integration test should use")
        val region = System.getenv("AWS_REGION")
            ?: System.getenv("AWS_DEFAULT_REGION")
            ?: "us-east-1"
        val endpoint = System.getenv("DYNAMODB_ENDPOINT")
        val client = DynamoDbAsyncClient.builder().apply {
            region(Region.of(region))
            if (!endpoint.isNullOrBlank()) {
                endpointOverride(URI.create(endpoint))
                if (System.getenv("AWS_ACCESS_KEY_ID").isNullOrBlank()) {
                    credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy"))
                    )
                }
            }
        }.build()

        val config = DynamoDbConfig(client, table, "it-${UUID.randomUUID()}")
        val store = DynamoDbKeyValueStore(config)
        runBlocking {
            createTableIfNeeded(config)
            try {
                store.putStringValue("account/1", "makowski")
                assertThat(store.getObjectAsString("account/1")).isEqualTo("makowski")

                store.putObjectAsJson("account/2", Person("krz", "makowski"))
                assertThat(store.getObject("account/2", Person::class.java))
                    .isEqualTo(Person("krz", "makowski"))

                store.putStringValue("other", "leave-me")
                assertThat(store.listKeys("account/")).containsExactly("account/1", "account/2")

                store.deleteObject("account/")
                assertThat(store.listKeys("")).containsExactly("other")
            } finally {
                store.deleteObject("")
                client.close()
            }
        }
    }

    data class Person(val first: String, val last: String)
}
