import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class MachineLearning {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("test")
                .config("spark.master", "local")
                .getOrCreate();

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"telling_sinds_lediging", "telling"})
                .setOutputCol("features");


        /*StructType schema = new StructType(new StructField[]{
                new StructField("datum", DataTypes.DateType, true, Metadata.empty()),
                new StructField("container_nr", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("container_afvaltype", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("lediging_24h_later", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("telling_sinds_lediging", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("telling", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, true, Metadata.empty())
        });*/
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
        System.out.println(binarySummary.areaUnderROC());
    }

}
