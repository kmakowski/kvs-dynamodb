# kvs-dynamodb

DynamoDB implementation of the [`kvs`](https://github.com/kmakowski/kvs) `KeyValueStore` interface.

```xml
<dependency>
    <groupId>io.github.kmakowski</groupId>
    <artifactId>kvs-dynamodb</artifactId>
    <version>1.0.0</version>
</dependency>
```

```kotlin
val client = DynamoDbAsyncClient.create()
val config = DynamoDbConfig(client, tableName = "kvs")
createTableIfNeeded(config)
val store = DynamoDbKeyValueStore(config)
```

The caller builds `DynamoDbAsyncClient` (region, credentials, and endpoint). `createTableIfNeeded` creates an on-demand table when `tableName` is missing and waits until it is active.

## Table

All objects for one store share a partition, so prefix listing is a `Query`.

| Attribute | Role |
| --- | --- |
| `pk` (String, hash key) | Store partition. Default `kv`, so several stores can share one table. |
| `sk` (String, range key) | Object key. |
| `value` (String) | Raw string, or JSON written by `putObjectAsJson`. |
| `updatedAt` (String) | Write time, ISO-8601. |

`listObjects` returns keys in sort-key order. `startAfterKey` is exclusive. `deleteObject(key)` removes every item whose key starts with `key`. An empty key removes the whole partition.

## Tests

`mvn test` runs against DynamoDB Local in Docker. The integration test is separate and talks to a table you name. It writes under a random partition and deletes that partition at the end.

```bash
DYNAMODB_TABLE=kvs AWS_REGION=eu-central-1 mvn -Pintegration verify
```

`DYNAMODB_ENDPOINT` overrides the client endpoint, for example `http://localhost:8000` when DynamoDB Local is already running. With an endpoint and no `AWS_ACCESS_KEY_ID`, the test uses dummy credentials.

Publish a release to Maven Central with a GPG key and a [Central Portal](https://central.sonatype.com) user token in `~/.m2/settings.xml` under server id `central`:

```bash
mvn deploy -Pcentral
```
