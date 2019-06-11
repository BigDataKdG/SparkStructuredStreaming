import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

/**
 * Consumer for the lediging container notifications.
 */

public class KafkaLedigingenConsumer {
    private final static String GROUP_ID = "sorteerstraatjes";
    private final static String BOOTSTRAP_SERVER = "localhost:9092";
    private final static String TOPIC = "ledigingen";

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "C:\\winutil\\");

        SparkSession spark = SparkSession
                .builder()
                .appName("ledigingen")
                .config("spark.master", "local")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        Dataset<Row> ledigingen = readLedigingen(spark, 11);

        StreamingQuery query2 = ledigingen.writeStream()
                .format("parquet")
                .option("truncate", "false")
                .option("checkpointLocation", "/kafka-logs2")
                .trigger(Trigger.ProcessingTime(10000))
                .start( "/kafka-ledigingen");

        query2.awaitTermination();
    }

    private static Dataset<Row> readLedigingen (SparkSession spark, Integer containerMeldingId){
        Dataset<Row> ledigingen = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
                .option("subscribe", TOPIC)
                .option("startingOffsets", "latest")
                .option("group.id", GROUP_ID)
                .option("failOnDataLoss", false)
                .option("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer")
                .option("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
                .load();

        return CountFromStream.getDatasetWithCounts(ledigingen, containerMeldingId);

    }

}
