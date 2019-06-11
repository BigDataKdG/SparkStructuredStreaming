import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Consumer for the lediging container notifications.
 */

public class KafkaStortingenConsumer {

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

        StreamingQuery query = stortingen.writeStream()
                .format("parquet")
                .option("truncate", "false")
                .option("checkpointLocation", "/kafka-logs")
                .trigger(Trigger.ProcessingTime(10000))
                .start( "/kafka-stortingen");
        query.awaitTermination();
    }

    private static Dataset<Row> readStream(SparkSession spark, Integer containerMeldingId) {
        Dataset<Row> stortingen = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
                .option("subscribe", "stortingen")
                .option("startingOffsets", "latest")
                .option("group.id", "sorteerstraatjes")
                .option("failOnDataLoss", false)
                .option("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer")
                .option("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
                .load();

        return CountFromStream.getDatasetWithCounts(stortingen, containerMeldingId);
    }


}
