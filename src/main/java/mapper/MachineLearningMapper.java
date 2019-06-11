package mapper;

import static java.util.Arrays.asList;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import model.LedigingenModel;
import model.MachineLearningDomain;
import model.StortingenModel;

/**
 * A mapper class to map the stortingen and ledigingen to a domain object that can be fed into the trained machine
 * learning model.
 */

public class MachineLearningMapper {

    public static List<MachineLearningDomain> machineLearningMapper (Dataset<Row> stortingen, Dataset<Row> ledigingen) {
        JavaRDD<StortingenModel> stortingenModel = stortingen.javaRDD()
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

        List<LedigingenModel> ledigingenModelList = new ArrayList<>(ledigingenModel.collect());
        List<StortingenModel> stortingenModelList = stortingenModel.collect();

        Collections.sort(ledigingenModelList, Comparator.comparing(LedigingenModel::getStartDate).reversed());
        for (int i = 0; i < ledigingenModelList.size(); i++) {
            if (i == 0) {
                ledigingenModelList.get(i).setEndDate(LocalDate.MAX);
            } else {
                LedigingenModel previous = ledigingenModelList.get(i - 1);
                ledigingenModelList.get(i).setEndDate(previous.getStartDate());
            }
        }

        // todo: check if this filter works
        //stortingenModelList.stream().filter(strtModel -> strtModel.getDate() == LocalDate.of(2018, 6, 1));


        return createMachineLearningModel(stortingenModelList, ledigingenModelList);
    }

    private static List<MachineLearningDomain> createMachineLearningModel(
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
