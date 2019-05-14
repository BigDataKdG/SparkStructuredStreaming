import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

public class Query {

    public static StreamingQuery startQuery(final Dataset<Row> df) {
        return df
                .writeStream()
                .format("memory")
                .queryName("mytable")
                .option("truncate", "false")
                .outputMode(OutputMode.Complete())
                .trigger(Trigger.ProcessingTime(15000))
                //.option("path", "/tmp")
                //.option("checkpointLocation", "/tmp")
                .start();
    }
}
