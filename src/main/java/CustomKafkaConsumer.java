import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

public class CustomKafkaConsumer {
    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("test")
                .config("spark.master", "local")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        /*Dataset<Row> parquet = spark.read().parquet("/tmp/part-00000-f6433dfc-ea36-4ad9-875e-4daeb18a325b-c000.snappy"
                + ".parquet");
        parquet.createOrReplaceTempView("parquet");
        Dataset<Row> sql = spark.sql("SELECT * from parquet");
        sql.show(100);*/

        Dataset<Row> df = spark.readStream()
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
                        + "as categorie")
                .selectExpr("split(date, '\"')[3] as date2", "split(nummer, '\"')[2] as nummer2",
                        "split(categorie, '\"')[3] as categorie2")
                .selectExpr("CAST(date2 AS TIMESTAMP) as timestamp", "split(nummer2, ':')[1] as container_nummer",
                        "categorie2 as categorie", "CAST(date2 AS DATE) as date")
                //.where("categorie == 'STRT'")
                .withWatermark("timestamp", "1 minutes")
                .groupBy(
                        functions.window(new Column("timestamp"), "1 day", "1 day"),
                        new Column("container_nummer"))
                .count();

        StreamingQuery query = df.writeStream()
               /* .format("console")
                .option("numRows", 1000)
                .option("truncate", false)
                .outputMode("complete")
                .start();*/
                .format("parquet")
                .option("truncate", "false")
                .option("checkpointLocation", "/tmp/kafka-logs")
                .start("/Users/JeBo/kafka-path");

        query.awaitTermination();
    }

}
