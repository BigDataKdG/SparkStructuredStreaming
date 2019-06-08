import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SingleFileFactory {

    public static void main(String[] args) throws Exception {

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

        /*df.explain();
        df.show(20);*/

    }

    public void machineLearning() {
        /*VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"telling_sinds_lediging", "telling"})
                .setOutputCol("features");

        Dataset<Row> training = spark.read()
                .option("header", true)
                .format("csv")
                //.schema(schema)
                .load("src/main/resources/ml_output.csv");
        training = training.drop("datum");
        training = training.drop("container_nr");
        training = training.drop("container_afvaltype");
        training = training.withColumn("lediging_24h_later",
                training.col("lediging_24h_later").cast(DataTypes.DoubleType));

        training = training.withColumn("telling_sinds_lediging",
                training.col("telling_sinds_lediging").cast(DataTypes.DoubleType));
        training = training.withColumn("telling", training.col("telling").cast(DataTypes.DoubleType));

        Dataset<Row> output = assembler.transform(training);

        //output = output.withColumn("features", output.col("features").cast(DataTypes.DoubleType));

        output = output.withColumnRenamed("lediging_24h_later", "label");
        output.select("features").show(false);


        Dataset<Row>[] trainAndTest = output.randomSplit(new double[]{0.9,0.1}, 2000L);

        Dataset<Row> train = trainAndTest[0];
        Dataset<Row> test = trainAndTest[1];

        System.out.println("Train set count: " + train.count());
        System.out.println("Test set count: " + test.count());

        LogisticRegression lr = new LogisticRegression().setMaxIter(10);
        LogisticRegressionModel lrModel = lr.fit(train);

        LogisticRegressionTrainingSummary trainingSummary = lrModel.summary();
        System.out.println(trainingSummary);
        BinaryLogisticRegressionSummary binarySummary =
                (BinaryLogisticRegressionSummary) trainingSummary;
        Dataset<Row> roc = binarySummary.roc();
        binarySummary.predictions().sort("prediction").filter("prediction > 0").show(10000);
        System.out.println(roc);
        System.out.println(binarySummary.areaUnderROC());*/
    }

}
