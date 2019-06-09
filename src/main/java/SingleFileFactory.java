import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SingleFileFactory {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("test")
                .config("spark.master", "local")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        Dataset<Row> stortingen = spark.read().parquet("/Users/JeBo/kafka-stortingen/part-*.snappy.parquet");

        stortingen.coalesce(1).write()
                .format("parquet").save("/Users/JeBo/single-parquet/single-file-stortingen.snappy.parquet");

        Dataset<Row> ledigingen = spark.read().parquet("/Users/JeBo/kafka-ledigingen/part-*.snappy.parquet");

        ledigingen.coalesce(1).write()
                .format("parquet").save("/Users/JeBo/single-parquet/single-file-ledigingen.snappy.parquet");

        //df.explain();
        stortingen.show(20, false);
        ledigingen.show(20);
    }
}
