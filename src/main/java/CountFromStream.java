import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Helper class to map the dataset to a dataset with the melding (either lediging or storting) counts for each
 * container for one day
 */

public class CountFromStream {

    public static Dataset<Row> getDatasetWithCounts(Dataset<Row> dataset, Integer containerMeldingId) {
        return dataset.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json")
                .selectExpr("split(json, ',')[0] as date", "split(json, ',')[2] as nummer", "split(json, ',')[1] "
                        + "as containerMeldingId", "split(json, ',')[3] as dayOfWeek")
                .selectExpr("split(date, '\"')[3] as date2", "split(nummer, '\"')[2] as nummer2",
                        "split(containerMeldingId, '\"')[2] as containerMeldingId2", "split(dayOfWeek, '\"')[2] as "
                                + "dayOfWeek2")
                .selectExpr("CAST(date2 AS TIMESTAMP) as timestamp", "split(nummer2, ':')[1] as container_nummer",
                        "CAST(date2 AS DATE) as date", "split(containerMeldingId2, ':')[1] as containerMeldingId"
                        , " "
                                + "split (dayOfWeek2, ':')[1] as dayOfWeek")
                .selectExpr("timestamp as timestamp", "container_nummer as container_nummer", "date as date",
                        "containerMeldingId as containerMeldingId", "split(dayOfWeek, '}')[0] as dayOfWeek")
                .where("containerMeldingId == '" + containerMeldingId + "'")
                .withWatermark("timestamp", "500 milliseconds")
                .groupBy(
                        functions.window(new Column("timestamp"), "1 day", "1 day"),
                        new Column("container_nummer"),
                        new Column("containerMeldingId"),
                        new Column("dayOfWeek"))
                .count();
    }
}
