import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

public class KafkaConsumer {

    private final static String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "C:\\winutil\\");

        SparkSession spark = SparkSession
                .builder()
                .appName("stortingen")
                .config("spark.master", "local")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        Dataset<Row> stortingen = readStream(spark, 21);

        String osName = System.getProperties().getProperty("os.name");
        String path;
        String checkpointLocation;
        if (osName.startsWith("Mac")) {
            path = "/Users/JeBo/";
            checkpointLocation = "/tmp/";
        } else {
            path = "C://";
            checkpointLocation = "tmp1/";
        }

        StreamingQuery query = stortingen.writeStream()
                /* .format("console")
                 .option("numRows", 1000)
                 .option("truncate", false)
                 .outputMode("complete")
                 .start();*/
                .format("parquet")
                .option("truncate", "false")
                .option("checkpointLocation", checkpointLocation + "kafka-logs")
                .trigger(Trigger.ProcessingTime(10000))
                .start(path + "kafka-stortingen");
        query.awaitTermination();
    }

    private static Dataset<Row> readStream(SparkSession spark, Integer containerMeldingId) {
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
                .option("subscribe", "stortingen")
                .option("startingOffsets", "latest")
                .option("group.id", "test")
                .option("failOnDataLoss", false)
                .option("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer")
                .option("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json")
                .selectExpr("split(json, ',')[0] as date", "split(json, ',')[2] as nummer", "split(json, ',')[1] "
                        + "as containerMeldingId", "split(json, ',')[3] as dayOfWeek")
                .selectExpr("split(date, '\"')[3] as date2", "split(nummer, '\"')[2] as nummer2",
                        "split(containerMeldingId, '\"')[2] as containerMeldingId2", "split(dayOfWeek, '\"')[2] as "
                                + "dayOfWeek2")
                .selectExpr("CAST(date2 AS TIMESTAMP) as timestamp", "split(nummer2, ':')[1] as container_nummer",
                        "CAST(date2 AS DATE) as date", "split(containerMeldingId2, ':')[1] as containerMeldingId", " "
                                + "split (dayOfWeek2, ':')[1] as dayOfWeek")
                .selectExpr("timestamp as timestamp", "container_nummer as container_nummer", "date as date",
                        "containerMeldingId as containerMeldingId", "split(dayOfWeek, '}')[0] as dayOfWeek")
                .where("containerMeldingId == '" + containerMeldingId + "'")
                .withWatermark("timestamp", "1 minutes")
                .groupBy(
                        functions.window(new Column("timestamp"), "1 day", "1 day"),
                        new Column("container_nummer"),
                        new Column("containerMeldingId"),
                        new Column("dayOfWeek"))
                .count();
    }


}
