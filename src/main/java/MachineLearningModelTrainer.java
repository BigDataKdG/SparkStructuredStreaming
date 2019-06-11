import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *  This class trains a Gradient Boosted Trees model using the Spark ML library. The model is saved to the target
 *  folder where it is later used in the Machine Learning Service in order to evaluate new data.
 */

public class MachineLearningModelTrainer {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("machineLearningModelTrainer")
                .config("spark.master", "local")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        Dataset<Row> mlInput = spark.read()
                .format("libsvm")
                .load("src/main/resources/out.txt");

        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(mlInput);

        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(700)
                .fit(mlInput);

        Dataset<Row>[] splits = mlInput.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        GBTClassifier gbt = new GBTClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures")
                .setMaxBins(700)
                .setMaxIter(100)
                .setStepSize(0.3);

        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, gbt, labelConverter});

        PipelineModel pipelineModel = pipeline.fit(trainingData);

        Dataset<Row> pipelinepredictions = pipelineModel.transform(testData);

        pipelinepredictions.select("predictedLabel", "label", "features")
                .show(100, false);

        BinaryClassificationEvaluator rocEvaluator = new BinaryClassificationEvaluator()
                .setMetricName("areaUnderROC")
                .setLabelCol("indexedLabel")
                .setRawPredictionCol("rawPrediction");

        double areaUnderROC = rocEvaluator.evaluate(pipelinepredictions);
        System.out.println("Area under ROC: " + areaUnderROC);

        pipelineModel.save("target/tmp/PipelineModel");
    }

}
