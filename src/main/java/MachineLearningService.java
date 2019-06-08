import static java.util.Arrays.asList;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import model.LedigingenModel;
import model.MachineLearningDomain;
import model.StortingenModel;

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
                .toDF("window", "container_nr", "count");

        Dataset<Row> ledigingen = spark.read()
                .parquet("/Users/JeBo/single-parquet/single-file-ledigingen.snappy.parquet")
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


        JavaRDD<LedigingenModel> ledigingenModel = ledigingen.javaRDD()
                .map((Function<Row, LedigingenModel>) s -> {
                    LedigingenModel model = new LedigingenModel();
                    model.setContainerNummer(s.getString(s.fieldIndex("container_nr")));
                    Row row = s.getStruct(s.fieldIndex("window"));
                    Timestamp ts = row.getTimestamp(0);
                    model.setStartDate(ts.toLocalDateTime().toLocalDate());
                    Timestamp ts2 = row.getTimestamp(1);
                    //model.setCount(s.getLong(s.fieldIndex("count")));
                    return model;
                });

        List<LedigingenModel> ledigingenModelList = ledigingenModel.collect();
        ledigingenModelList.stream().forEach(System.out::println);

        List<StortingenModel> list = df2.collect();
        list.stream().forEach(System.out::println);


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

        List<MachineLearningDomain> model = createMachineLearningModel(stortingenList, ledigingenList);

    }


    public static List<MachineLearningDomain> createMachineLearningModel(List<StortingenModel> stortingenList,
                                                                         List<LedigingenModel> ledigingenList) {
        List<MachineLearningDomain> machineLearningDomainList = new ArrayList<>();
       List<StortingenModel> sortedStortingen =
               stortingenList.stream().sorted(Comparator.comparing(StortingenModel::getDate)).collect(Collectors.toList());

        for (StortingenModel stortingenModel : sortedStortingen) {
            MachineLearningDomain model = MachineLearningDomain.builder()
                    .containerNummer(stortingenModel.getContainerNummer())
                    .volume(stortingenModel.getCount())
                    .volumeSindsLaatsteLediging(getVolumeSindsLaatsteLediging(stortingenModel, sortedStortingen,
                            ledigingenList))
                    .build();
            machineLearningDomainList.add(model);
        }

        /*machineLearningDomainList.stream().sorted(Comparator.comparing(MachineLearningDomain::getContainerNummer))
                .forEach(System.out::println);*/
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
