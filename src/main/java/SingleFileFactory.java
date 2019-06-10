import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SingleFileFactory {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\winutil\\");

        SparkSession spark = SparkSession
                .builder()
                .appName("test")
                .config("spark.master", "local")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
        String osName = System.getProperties().getProperty("os.name");
        String stortingenPath;
        String ledigingenPath;
        String singleFilePath;
        if (osName.startsWith("Mac")) {
            stortingenPath = "/Users/JeBo/kafka-stortingen/";
            singleFilePath = "/Users/JeBo/single-parquet/";
            ledigingenPath = "/Users/JeBo/kafka-ledigingen/";
        } else {
            stortingenPath = "C://kafka-stortingen/";
            singleFilePath = "C://single-parquet/";
            ledigingenPath = "C://kafka-ledigingen/";
        }

        Dataset<Row> stortingen = spark.read().parquet(stortingenPath + "part-*.snappy.parquet");

        stortingen.coalesce(1).write()
                .format("parquet").save(singleFilePath+ "single-file-stortingen.snappy.parquet");

        Dataset<Row> ledigingen = spark.read().parquet(ledigingenPath + "part-*.snappy.parquet");

        ledigingen.coalesce(1).write()
                .format("parquet").save( singleFilePath + "single-file-ledigingen.snappy.parquet");

        //df.explain();
        stortingen.show(150, false);
        ledigingen.show(150);
    }
}
