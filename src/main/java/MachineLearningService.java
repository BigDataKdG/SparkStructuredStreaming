import static java.util.Arrays.asList;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.second;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.functions.when;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;

import model.ContainerPrediction;
import model.LedigingenModel;
import model.MachineLearningDomain;
import model.StortingenModel;
import scala.collection.Seq;

public class MachineLearningService {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("test")
                .config("spark.master", "local")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .parquet("/Users/JeBo/single-parquet/single-file-stortingen.snappy.parquet")
                .toDF("window", "container_nr", "containerMeldingId", "dayOfWeek", "count");

        Dataset<Row> ledigingen = spark.read()
                .parquet("/Users/JeBo/single-parquet/single-file-ledigingen.snappy.parquet")
                .toDF("window", "container_nr", "containerMeldingId", "dayOfWeek", "count");
        df.printSchema();

        JavaRDD<StortingenModel> stortingenModel = df.javaRDD()
                .map((Function<Row, StortingenModel>) s -> {
                    StortingenModel model = new StortingenModel();
                    model.setContainerNummer(s.getString(s.fieldIndex("container_nr")));
                    Row row = s.getStruct(s.fieldIndex("window"));
                    Timestamp ts = row.getTimestamp(0);
                    model.setDate(ts.toLocalDateTime().toLocalDate());
                    model.setContainerMeldingCategorie(s.getString(s.fieldIndex("containerMeldingId")));
                    model.setDayOfWeek(s.getString(s.fieldIndex("dayOfWeek")));
                    model.setCount(s.getLong(s.fieldIndex("count")) * 30);
                    return model;
                });


        JavaRDD<LedigingenModel> ledigingenModel = ledigingen.javaRDD()
                .map((Function<Row, LedigingenModel>) s -> {
                    LedigingenModel model = new LedigingenModel();
                    model.setContainerNummer(s.getString(s.fieldIndex("container_nr")));
                    Row row = s.getStruct(s.fieldIndex("window"));
                    Timestamp ts = row.getTimestamp(0);
                    model.setStartDate(ts.toLocalDateTime().toLocalDate());
                    model.setContainerMeldingCategorie(s.getString(s.fieldIndex("containerMeldingId")));
                    model.setDayOfWeek(s.getString(s.fieldIndex("dayOfWeek")));
                    return model;
                });

        // create modifiable list
        List<LedigingenModel> ledigingenModelList = new ArrayList<>(ledigingenModel.collect());
        Collections.sort(ledigingenModelList, Comparator.comparing(LedigingenModel::getStartDate).reversed());
        for (int i = 0; i < ledigingenModelList.size(); i++) {
            if (i == 0) {
                ledigingenModelList.get(i).setEndDate(LocalDate.MAX);
            } else {
                LedigingenModel previous = ledigingenModelList.get(i - 1);
                ledigingenModelList.get(i).setEndDate(previous.getStartDate());
            }
        }

        ledigingenModelList.stream().forEach(System.out::println);

        List<StortingenModel> stortingenModelList = stortingenModel.collect();

        // todo: check if this filter works
        //stortingenModelList.stream().filter(strtModel -> strtModel.getDate() == LocalDate.of(2018, 6, 1));

        List<MachineLearningDomain> model = createMachineLearningModel(stortingenModelList, ledigingenModelList);

        Dataset<Row> datasetML = spark.createDataFrame(model, MachineLearningDomain.class);

        PipelineModel pipelineModel = PipelineModel.load("/Users/JeBo/BigData2/SparkStructuredStreaming/target/tmp"
                + "/PipelineModel");

        String[] featureCols = new String[]{"containerNummer", "volumeSindsLaatsteLediging", "volume", "dayOfWeek"};

        VectorAssembler assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features");

        System.out.println("NEW DATASET ML PRINT SCHEMA");

        Dataset<Row> df2 = assembler.transform(datasetML);
        System.out.println("DF3 PRINT SCHEMA");

        df2.printSchema();

        Dataset<Row> pipelinepredictions = pipelineModel.transform(df2);
        System.out.println("PIPELINE PREDICTIONS SCHEMA");
        pipelinepredictions.printSchema();
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
        finalList.forEach(System.out::println);
    }


    public static List<MachineLearningDomain> createMachineLearningModel(
            List<StortingenModel> stortingenList,
            List<LedigingenModel> ledigingenList) {
        List<MachineLearningDomain> machineLearningDomainList = new ArrayList<>();
        List<StortingenModel> sortedStortingen =
                stortingenList.stream().sorted(Comparator.comparing(StortingenModel::getDate)).collect(Collectors.toList());

        for (StortingenModel stortingenModel : sortedStortingen) {
            MachineLearningDomain model = MachineLearningDomain.builder()
                    .containerNummer(Integer.parseInt(stortingenModel.getContainerNummer()))
                    .volume(stortingenModel.getCount())
                    .volumeSindsLaatsteLediging(getVolumeSindsLaatsteLediging(stortingenModel, sortedStortingen,
                            ledigingenList))
                    .dayOfWeek(Integer.valueOf(stortingenModel.getDayOfWeek()))
                    .build();
            machineLearningDomainList.add(model);
        }

        machineLearningDomainList.stream().sorted(Comparator.comparing(MachineLearningDomain::getContainerNummer))
                .forEach(System.out::println);
        return machineLearningDomainList;
    }

    private static Long getVolumeSindsLaatsteLediging(
            final StortingenModel stortingenModel, final List<StortingenModel> stortingenList,
            final List<LedigingenModel> ledigingenList) {
        List<LocalDate> timeRange = getTimeRange(stortingenModel, ledigingenList);

        return stortingenList.stream()
                .filter(model -> stortingenModel.getContainerNummer().equals(model.getContainerNummer()))
                .filter(model -> model.getDate().isBefore(timeRange.get(0)) && model.getDate().isAfter(timeRange.get(1)))
                .filter(model -> model.getDate().isBefore(stortingenModel.getDate()) || model.getDate().isEqual(stortingenModel.getDate()))
                .mapToLong(model -> model.getCount())
                .sum();
    }

    private static List<LocalDate> getTimeRange(
            final StortingenModel stortingenModel,
            final List<LedigingenModel> ledigingenList) {
        LocalDate endDate = ledigingenList.stream()
                .filter(lediging -> stortingenModel.getDate() != null &&
                        stortingenModel.getDate().isAfter(lediging.getStartDate()) &&
                        stortingenModel.getDate().isBefore(lediging.getEndDate()))
                .map(lediging -> lediging.getEndDate())
                .findFirst().orElse(LocalDate.MAX);
        LocalDate startdate = ledigingenList.stream()
                .filter(lediging -> stortingenModel.getDate().isAfter(lediging.getStartDate())
                        && stortingenModel.getDate().isBefore(lediging.getEndDate()))
                .map(lediging -> lediging.getStartDate())
                .findFirst().orElse(LocalDate.MIN);
        return asList(endDate, startdate);
    }
}