import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class Main {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .appName("testKafka")
                .config("spark.master", "local")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .getOrCreate();

        Dataset<Row> ds1 = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .option("startingOffsets", "earliest")
                .load();

        StreamingQuery query = ds1.writeStream()
                .format("parquet")
                .option("checkpointLocation", "test2")
                .option("path", "test")
                .start();

         query.awaitTermination();
    }
}
