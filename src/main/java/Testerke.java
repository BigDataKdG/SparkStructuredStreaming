import static java.util.Arrays.asList;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import model.LedigingenModel;
import model.MachineLearningModel;
import model.StortingenModel;

public class Testerke {

    public static void main(String[] args) {
       /* SparkSession spark = SparkSession
                .builder()
                .appName("test")
                .config("spark.master", "local")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .parquet("/Users/JeBo/single-parquet/single-file.snappy.parquet")
                .toDF("window", "container_nr", "count");

        Dataset<Row> ledigingen = spark.read()
                .parquet("/Users/JeBo/single-parquet/single-file-ledi.snappy.parquet")
                .toDF("window", "container_nr", "count");
        df.printSchema();

        JavaRDD<StortingenModel> df2 = df.javaRDD()
                .map((Function<Row, StortingenModel>) s -> {
                    StortingenModel mlm = new StortingenModel();
                    mlm.setContainerNummer(s.getString(s.fieldIndex("container_nr")));
                    Row row = s.getStruct(s.fieldIndex("window"));
                    Timestamp ts = row.getTimestamp(0);
                    mlm.setDate(ts.toLocalDateTime().toLocalDate());
                    mlm.setCount(s.getLong(s.fieldIndex("count")));
                    return mlm;
                });
*/



        /*String datapath = "/Users/JeBo/BigData2/SparkStructuredStreaming/ml_input.csv";
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(spark.sparkContext(), datapath).toJavaRDD();

        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

// Train a GradientBoostedTrees model.
// The defaultParams for Regression use SquaredError by default.
        BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Regression");
        boostingStrategy.setNumIterations(3); // Note: Use more iterations in practice.
        boostingStrategy.getTreeStrategy().setMaxDepth(5);
// Empty categoricalFeaturesInfo indicates all features are continuous.
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);

        GradientBoostedTreesModel model = GradientBoostedTrees.train(trainingData, boostingStrategy);

// Evaluate model on test instances and compute test error
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));

        model.toDebugString();
        //List<StortingenModel> list = df2.collect();
        //list.stream().forEach(System.out::println);

*/
        // STORTINGEN
        List<StortingenModel> stortingenList = new ArrayList<>();
        StortingenModel strt1 = buildStortingenModel("466", 26L, LocalDate.of(2018, 12, 5),
                "STRT");
        StortingenModel strt2 = buildStortingenModel("466", 31L, LocalDate.of(2018, 12, 6),
                "STRT");
        StortingenModel strt3 = buildStortingenModel("466", 25L, LocalDate.of(2018, 12, 7),
                "STRT");
        StortingenModel strt4 = buildStortingenModel("466", 21L, LocalDate.of(2018, 12, 11),
                "STRT");
        StortingenModel strt5 = buildStortingenModel("466", 25L, LocalDate.of(2018, 12, 12),
                "STRT");

        StortingenModel strt7 = buildStortingenModel("322", 26L, LocalDate.of(2018, 12, 5),
                "STRT");
        StortingenModel strt8 = buildStortingenModel("322", 31L, LocalDate.of(2018, 12, 6),
                "STRT");
        StortingenModel strt6 = buildStortingenModel("322", 25L, LocalDate.of(2018, 12, 12),
                "STRT");

        List<LedigingenModel> ledigingenList = new ArrayList<>();

        // LEDIGINGEN
        LedigingenModel ledi1 = buildLedigingenModel("466", LocalDate.of(2018, 12, 4),
                LocalDate.of(2018, 12, 10));
        LedigingenModel ledi2 = buildLedigingenModel("466", LocalDate.of(2018, 12, 10),
                LocalDate.of(2018, 12, 13));
        LedigingenModel ledi3 = buildLedigingenModel("466", LocalDate.of(2018, 12, 13),
                LocalDate.of(2018, 12, 16));


        LedigingenModel ledi4 = buildLedigingenModel("322", LocalDate.of(2018, 12, 4),
                LocalDate.of(2018, 12, 10));
        LedigingenModel ledi5 = buildLedigingenModel("322", LocalDate.of(2018, 12, 10),
                LocalDate.of(2018, 12, 13));
        LedigingenModel ledi6 = buildLedigingenModel("322", LocalDate.of(2018, 12, 13),
                LocalDate.of(2018, 12, 16));

        stortingenList.add(strt1);
        stortingenList.add(strt2);
        stortingenList.add(strt3);
        stortingenList.add(strt4);
        stortingenList.add(strt5);
        stortingenList.add(strt6);
        stortingenList.add(strt7);
        stortingenList.add(strt8);

        ledigingenList.add(ledi1);
        ledigingenList.add(ledi2);
        ledigingenList.add(ledi3);
        ledigingenList.add(ledi4);
        ledigingenList.add(ledi5);
        ledigingenList.add(ledi6);

        createMachineLearningModel(stortingenList, ledigingenList);
    }


    public static List<MachineLearningModel> createMachineLearningModel(List<StortingenModel> stortingenList,
                                                            List<LedigingenModel> ledigingenList) {
        List<MachineLearningModel> machineLearningModelList = new ArrayList<>();
       List<StortingenModel> sortedStortingen =
               stortingenList.stream().sorted(Comparator.comparing(StortingenModel::getDate)).collect(Collectors.toList());

        for (StortingenModel stortingenModel : sortedStortingen) {
            MachineLearningModel model = MachineLearningModel.builder()
                    .containerNummer(stortingenModel.getContainerNummer())
                    .volume(stortingenModel.getCount())
                    .volumeSindsLaatsteLediging(getVolumeSindsLaatsteLediging(stortingenModel, sortedStortingen,
                            ledigingenList))
                    .build();
            machineLearningModelList.add(model);
        }

        machineLearningModelList.stream().sorted(Comparator.comparing(MachineLearningModel::getContainerNummer))
                .forEach(System.out::println);
        return machineLearningModelList;
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

    private static List<LocalDate> getTimeRange(final StortingenModel stortingenModel,
                                                final List<LedigingenModel> ledigingenList) {
        LocalDate endDate = ledigingenList.stream()
                .filter(lediging -> stortingenModel.getDate().isAfter(lediging.getStartDate())
                        && stortingenModel.getDate().isBefore(lediging.getEndDate()))
                .map(lediging -> lediging.getEndDate())
                .findFirst().get();
        LocalDate startdate = ledigingenList.stream()
                .filter(lediging -> stortingenModel.getDate().isAfter(lediging.getStartDate())
                        && stortingenModel.getDate().isBefore(lediging.getEndDate()))
                .map(lediging -> lediging.getStartDate())
                .findFirst()
                .get();
        return asList(endDate, startdate);
    }

    private static StortingenModel buildStortingenModel(
            final String containerNr, final Long count, final LocalDate date,
            final String containerMeldingCategorie) {
        return StortingenModel.builder()
                .container_nr(containerNr)
                .count(count)
                .window(date)
                .containerMeldingCategorie(containerMeldingCategorie)
                .build();


    }

    private static LedigingenModel buildLedigingenModel(
            final String containerNr, final LocalDate startDate, final LocalDate endDate) {
        return LedigingenModel.builder()
                .containerNummer(containerNr)
                .startDate(startDate)
                .endDate(endDate)
                .build();


    }

}
