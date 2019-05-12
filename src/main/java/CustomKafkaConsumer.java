import static org.apache.spark.sql.functions.from_json;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class CustomKafkaConsumer {
    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("test")
                .config("spark.master", "local")
                .getOrCreate();

        StructType schema = new StructType()
                .add("containerActiviteit", DataTypes.DateType)
                .add("containerNummer", DataTypes.IntegerType)
                .add("containerMeldingCategorie", DataTypes.StringType);

        spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
                .option("subscribe", "test")
                .option("group.id", "test")
                .option("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer")
                .option("value.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json")
                .select(from_json(new Column("json"), schema).as("data"))
                .where("data.containerMeldingCategorie = 'STRT'")
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start()
                .awaitTermination();

        //Dataset<ContainerMeldingConsumer> df2 = df.as(Encoders.bean(ContainerMeldingConsumer.class));
        //Dataset<Row> df2 = df.select(from_json(new Column("json"), schema).as("data"));


        /*df.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .start()
                .awaitTermination();
*/
        /*while (true) {
            ConsumerRecords<Integer, ContainerMelding> records = consumer.poll(100);

            for (ConsumerRecord<Integer, ContainerMelding> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

            }
        }*/

    }

}
