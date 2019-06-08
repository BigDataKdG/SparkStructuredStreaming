import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;

public class KafkaConsumer {
    private final static String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("test")
                .config("spark.master", "local")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        // todo: add containermelding_id ipv categorie
        // todo: extra count voor stortingen (kleine vs grote storting)
        Dataset<Row> stortingen = readStream(spark, "20");
        Dataset<Row> ledigingen = readStream(spark, "11");

        StreamingQuery query = stortingen.writeStream()
                /* .format("console")
                 .option("numRows", 1000)
                 .option("truncate", false)
                 .outputMode("complete")
                 .start();*/
                .format("parquet")
                .option("truncate", "false")
                .option("checkpointLocation", "/tmp/kafka-logs")
                .start("/Users/JeBo/kafka-stortingen");


        StreamingQuery query2 = ledigingen.writeStream()
                /* .format("console")
                 .option("numRows", 1000)
                 .option("truncate", false)
                 .outputMode("complete")
                 .start();*/
                .format("parquet")
                .option("truncate", "false")
                .option("checkpointLocation", "/tmp/kafka-logs2")
                .start("/Users/JeBo/kafka-ledigingen");

        query.awaitTermination();
    }

    private static Dataset<Row> readStream(SparkSession spark, String containerMeldingId) {
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
                .option("subscribe", "test")
                .option("startingOffsets", "latest")
                .option("group.id", "test")
                .option("failOnDataLoss", false)
                .option("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer")
                .option("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json")
                //.select(from_json(new Column("json"), schema).as("json"))
                .selectExpr("split(json, ',')[0] as date", "split(json, ',')[1] as nummer", "split(json, ',')[2] "
                + "as dayOfWeek", "split(json, ',')[3] as containerMeldingId")
                .selectExpr("split(date, '\"')[3] as date2", "split(nummer, '\"')[2] as nummer2",
                        "split(dayOfWeek, '\"')[2] as dayOfWeek2", "split(containerMeldingId, ',')[2] as "
                                + "containerMeldingId2")
                .selectExpr("CAST(date2 AS TIMESTAMP) as timestamp", "split(nummer2, ':')[1] as container_nummer",
                       "CAST(date2 AS DATE) as date", "dayOfWeek2 as dayOfWeek", "containerMeldingId2 as containerMeldingId")
                .where("containerMeldingId == '" + containerMeldingId + "'")
                .withWatermark("timestamp", "1 minutes")
                .groupBy(
                        functions.window(new Column("timestamp"), "1 day", "1 day"),
                        new Column("container_nummer"))
                .count();
    }

}
