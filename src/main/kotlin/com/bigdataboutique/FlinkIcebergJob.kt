package com.bigdataboutique

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.aws.AwsProperties
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.flink.CatalogLoader
import org.apache.iceberg.flink.TableLoader
import org.apache.iceberg.flink.sink.FlinkSink
import org.apache.iceberg.types.Types
import java.time.Duration
import org.apache.hadoop.conf.Configuration as HadoopConfiguration

fun runJob(env: StreamExecutionEnvironment) {
    /* TODO: Checkpoints effectively define how often to commit files.
        Larger files are better but we need to also make sure we risk overflowing the buffers/heap.
        they can also be compacted retroactively in Athena for example.
       */
    env.enableCheckpointing(
        Duration.ofMinutes(5).toMillis(),
        CheckpointingMode.AT_LEAST_ONCE
    )

    /* TODO: I just use the default Hadoop conf, but here you can modify some of the filesystem properties. (Aliased import again)
        The defaults are usually fine but you can just modify those using the catalog properties definition below.
        Might need to play with max connections, size of each multipart upload etc.
     */
    val hadoopConf = HadoopConfiguration()

    /* TODO: Catalog init. This calls Flink's CREATE CATALOG statement internally.
    *   io-impl shouldn't change unless using a different provider than AWS
    *   warehouse is just the root location under which new tables will be created in s3 (unless a specific location is specified).
    *
    *   Other properties:
    *   cache-enabled - default true
    *   cache.expiration-interval-ms default is -1 (never)
    *   I suggest to look at AwsProperties.class for the aws configurable properties. (Max connections etc)
    *  */
    val catalogProperties: MutableMap<String, String> = HashMap()
    catalogProperties["io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"
    catalogProperties["warehouse"] = "s3://lior-bdbq-test-us-east-1/data/"

    // TODO: Have to use Apache HTTP client otherwise the AWS calls to authenticate just fail.(Default is URLConnection)
    catalogProperties[AwsProperties.HTTP_CLIENT_TYPE] = AwsProperties.HTTP_CLIENT_TYPE_APACHE

    val catalogLoader = CatalogLoader.custom(
        "demo",
        catalogProperties,
        hadoopConf,
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )

    val catalog: Catalog = catalogLoader.loadCatalog()

    val databaseName = "default"
    val tableName = "iceberg_flink_table_test"
    val outputTable = TableIdentifier.of(
        databaseName,
        tableName
    )
    val schema = Schema(
        Types.NestedField.required(1, "str1", Types.StringType.get()),
        Types.NestedField.required(2, "str2", Types.StringType.get()),
        Types.NestedField.required(3, "event_time", Types.TimestampType.withoutZone())
    )

    // FIXME: Table creation. Doesn't have to be part of the job.
    if (!catalog.tableExists(outputTable)) {
        catalog.createTable(
            outputTable,
            schema,
            PartitionSpec.builderFor(schema).day("event_time").build(),
            "s3://lior-bdbq-test-us-east-1/data/${tableName}", // TODO: Location. Optional property. If omitted it will just create the table under the DB folder.
            mapOf<String, String>(
                "write.format.default" to "parquet",
                "write.target-file-size-bytes" to "536870912", // Target file sizes when running optimize etc.
                "commit.manifest.min-count-to-merge" to "100", // How frequently to merge metadata files. 100 is fine for most use cases.
                "write.parquet.compression-codec" to "gzip", // Sometimes you'll want to go with a less aggressive but faster compression like ZSTD
                "write.distribution-mode" to "none" // Should flink shuffle data around per partition (creates fewer files)
            ) // TODO: Map of table properties. Full list here: https://iceberg.apache.org/docs/latest/configuration/#table-properties
        )
    }

    /* TODO: Below is the bare minimum to get a working "append" to iceberg.
        Source is usually Kafka/Kinesis or whatever stream.
        The important part is to create a RowData object that is compatible with the schema.
    */
    val stream = env.addSource(RowDataProducingSource(), "Source")

    /* TODO: Flink sink properties can overwrite some of the table properties above such as target-file-size-bytes etc. List here
        https://iceberg.apache.org/docs/latest/flink/#write-options
    */
    FlinkSink.forRowData(stream)
        .tableLoader(TableLoader.fromCatalog(catalogLoader, outputTable))
        .append()
        .name("Sink");
    /* TODO note - if you want to convert into Row/RowData as part of the sync you can use a custom builder..
        I find this a hassle, easier to do earlier in my own step.
        FlinkSink.builderFor(input, mapFunction that returns RowData, RowData type information)...
    */

    // Start the job..
    env.execute("Iceberg append job")
}
