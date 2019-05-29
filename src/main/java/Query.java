import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

public class Query {

    public static StreamingQuery startQuery(final Dataset<Row> df) {
        return df
                .repartition(1)
                .writeStream()
                .format("parquet")
                .option("truncate", "false")
                .trigger(Trigger.ProcessingTime(10000))
                //.option("path", "/Users/JeBo/kafka-path")
                .option("checkpointLocation", "/tmp/kafka-logs")
                .start("/Users/JeBo/kafka-path");
    }
}
