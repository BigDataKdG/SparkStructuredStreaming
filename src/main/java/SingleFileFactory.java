import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This class gathers all generated parquet files for stortingen and ledigingen, and groups them into one parquet
 * file for each
 */

public class SingleFileFactory {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\winutil\\");

        SparkSession spark = SparkSession
                .builder()
                .appName("singleFileFactory")
                .config("spark.master", "local")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        Dataset<Row> stortingen = spark.read().parquet("kafka-stortingen/part-*.snappy.parquet");

        stortingen.coalesce(1).write()
                .format("parquet").save( "single-parquet/single-file-stortingen.snappy.parquet");

        Dataset<Row> ledigingen = spark.read().parquet("kafka-ledigingen/part-*.snappy.parquet");

        ledigingen.coalesce(1).write()
                .format("parquet").save("single-parquet/single-file-ledigingen.snappy.parquet");

        stortingen.show(200, false);
        ledigingen.show(200, false);
    }
}
