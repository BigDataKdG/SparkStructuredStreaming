import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;

public class CustomKafkaConsumer {
    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("test")
                .config("spark.master", "local")
                .getOrCreate();

        /*Dataset<Row> parquet = spark.read().parquet("/tmp/part-00000-f6433dfc-ea36-4ad9-875e-4daeb18a325b-c000.snappy"
                + ".parquet");
        parquet.createOrReplaceTempView("parquet");
        Dataset<Row> sql = spark.sql("SELECT * from parquet");
        sql.show(100);*/

        spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
                .option("subscribe", "test")
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
                .withWatermark("timestamp", "10 minutes")
                .groupBy(
                        functions.window(new Column("timestamp"), "720 minutes", "720 minutes")
                        )
                .count()
                .writeStream()
                .format("console")
                //.option("path", "/tmp")
                //.option("checkpointLocation", "/tmp")
                .start()
                .awaitTermination();

        /*while (true) {
            ConsumerRecords<Integer, ContainerMelding> records = consumer.poll(100);

            for (ConsumerRecord<Integer, ContainerMelding> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

            }
        }*/

    }

}
