import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import com.google.common.hash.Hashing;

import scala.Tuple2;

public class MachineLearningModelTrainer {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("test")
                .config("spark.master", "local")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(
                spark.sparkContext(),
                "/Users/JeBo/BigData2/SparkStructuredStreaming/out.txt").toJavaRDD();
        spark.sparkContext().setCheckpointDir("/Users/JeBo/BigData2/SparkStructuredStreaming/target/checkpoint");

        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

        // Train a GradientBoostedTrees model.
        // The defaultParams for Regression use SquaredError by default.
        BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Classification");
        boostingStrategy.setNumIterations(500);
        boostingStrategy.getTreeStrategy().setNumClasses(2);
        boostingStrategy.getTreeStrategy().setMaxDepth(5);
        boostingStrategy.treeStrategy().setCheckpointInterval(50);
        boostingStrategy.treeStrategy().setMinInstancesPerNode(15);
        boostingStrategy.setLearningRate(0.3);
        boostingStrategy.treeStrategy().setMaxBins(810);

        // Empty categoricalFeaturesInfo indicates all features are continuous.
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        categoricalFeaturesInfo.put(0, 810);
        boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);

        GradientBoostedTreesModel model = GradientBoostedTrees.train(trainingData, boostingStrategy);

        // Evaluate model on test instances and compute test error
        JavaPairRDD<Object, Object> predictionAndLabel =
                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));

        BinaryClassificationMetrics metrics =
                new BinaryClassificationMetrics(predictionAndLabel.rdd());

        // Precision by threshold
        JavaRDD<Tuple2<Object, Object>> precision = metrics.precisionByThreshold().toJavaRDD();
        System.out.println("Precision by threshold: " + precision.collect());

        // Recall by threshold
        JavaRDD<?> recall = metrics.recallByThreshold().toJavaRDD();
        System.out.println("Recall by threshold: " + recall.collect());

        // F Score by threshold
        JavaRDD<?> f1Score = metrics.fMeasureByThreshold().toJavaRDD();
        System.out.println("F1 Score by threshold: " + f1Score.collect());

        JavaRDD<?> f2Score = metrics.fMeasureByThreshold(2.0).toJavaRDD();
        System.out.println("F2 Score by threshold: " + f2Score.collect());

        // Precision-recall curve
        JavaRDD<?> prc = metrics.pr().toJavaRDD();
        System.out.println("Precision-recall curve: " + prc.collect());

        // Thresholds
        JavaRDD<Double> thresholds = precision.map(t -> Double.parseDouble(t._1().toString()));

        // ROC Curve
        JavaRDD<?> roc = metrics.roc().toJavaRDD();

        System.out.println("Area under ROC = " + metrics.areaUnderROC());

        //System.out.println("model to debug string:" + model.toDebugString());
        model.save(spark.sparkContext(), "target/tmp/myGradientBoostingClassificationModel");
    }

}
