import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Testerke {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("test")
                .config("spark.master", "local")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .parquet("/Users/JeBo/single-parquet/single-file.snappy.parquet");
        df.show(10000, false);

    }
}
