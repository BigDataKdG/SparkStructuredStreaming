import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;

import mapper.MachineLearningMapper;
import model.ContainerPrediction;
import model.MachineLearningDomain;

/**
 * In this service class, the model that was trained in the MachineLearningModelTrainer is applied to new data.
 */

public class MachineLearningService {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("machineLearningService")
                .config("spark.master", "local")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        Dataset<Row> stortingen = spark.read()
                .parquet("single-parquet/single-file-stortingen.snappy.parquet")
                .toDF("window", "container_nr", "containerMeldingId", "dayOfWeek", "count");

        Dataset<Row> ledigingen = spark.read()
                .parquet("single-parquet/single-file-ledigingen.snappy.parquet")
                .toDF("window", "container_nr", "containerMeldingId", "dayOfWeek", "count");

        List<MachineLearningDomain> machineLearningDomain = MachineLearningMapper.machineLearningMapper(stortingen,
                ledigingen);

        // create modifiable list
        Dataset<Row> datasetML = spark.createDataFrame(machineLearningDomain, MachineLearningDomain.class);

        PipelineModel pipelineModel = PipelineModel.load("target/tmp/PipelineModel");

        String[] featureCols = new String[]{"containerNummer", "volumeSindsLaatsteLediging", "volume", "dayOfWeek"};
        VectorAssembler assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features");

        Dataset<Row> transformedVectors = assembler.transform(datasetML);
        Dataset<Row> pipelinepredictions = pipelineModel.transform(transformedVectors);

        List<ContainerPrediction> containerPredictions = prepareDatasetForExtraction(pipelinepredictions);

        containerPredictions.forEach(System.out::println);

    }

    private static List<ContainerPrediction> prepareDatasetForExtraction(final Dataset<Row> pipelinepredictions) {
        UserDefinedFunction toarray = udf(
                (Vector v) -> v.toArray(), new ArrayType(DataTypes.DoubleType, false)
        );

        List<Row> rows = pipelinepredictions.select(col("predictedLabel"), toarray.apply(col("probability")),
                col("indexedFeatures"), col("containerNummer"))
                .collectAsList();

        List<Object> probabilities = rows
                .stream()
                .map(row -> row.getSeq(1))
                .map(row -> row.last())
                .collect(Collectors.toList());

        List<Integer> containerNummers = rows.stream()
                .map(row -> row.getInt(3))
                .collect(Collectors.toList());

        List<Double> probabilitiesAsDoubles = new ArrayList<>();

        for (Object obj : probabilities) {
            probabilitiesAsDoubles.add((Double) obj);
        }

        List<String> predictedLabels = new ArrayList<>();

        for (Double prob : probabilitiesAsDoubles) {
            if (prob > 0.169) {
                predictedLabels.add("Lediging");
            } else {
                predictedLabels.add("Geen lediging");
            }
        }

        List<ContainerPrediction> finalList = new ArrayList<>();
        for (int i = 0; i < containerNummers.size(); i++) {
            finalList.add(new ContainerPrediction(containerNummers.get(i), probabilitiesAsDoubles.get(i),
                    predictedLabels.get(i)));
        }
        return finalList;
    }

}