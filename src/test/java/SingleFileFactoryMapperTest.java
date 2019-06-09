import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import model.LedigingenModel;
import model.MachineLearningDomain;
import model.StortingenModel;

public class SingleFileFactoryMapperTest {

    @Test
    public void TEST_MAPPER() {
        List<MachineLearningDomain> modelList = MachineLearningService.createMachineLearningModel(getStortingen(), getLedigingen());
        assertThat(modelList).isEqualTo(getExpectedList());
    }

    //todo: implement expected list
    private List<MachineLearningDomain> getExpectedList() {
        return null;
    }

    private List<StortingenModel> getStortingen() {
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
        StortingenModel strt6 = buildStortingenModel("322", 25L, LocalDate.of(2018, 12, 12),
                "STRT");
        StortingenModel strt8 = buildStortingenModel("322", 31L, LocalDate.of(2018, 12, 6),
                "STRT");


        stortingenList.add(strt1);
        stortingenList.add(strt2);
        stortingenList.add(strt3);
        stortingenList.add(strt4);
        stortingenList.add(strt5);
        stortingenList.add(strt6);
        stortingenList.add(strt7);
        stortingenList.add(strt8);

        return stortingenList;
    }

    private List<LedigingenModel> getLedigingen() {
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

        ledigingenList.add(ledi1);
        ledigingenList.add(ledi2);
        ledigingenList.add(ledi3);
        ledigingenList.add(ledi4);
        ledigingenList.add(ledi5);
        ledigingenList.add(ledi6);

        return ledigingenList;
    }

    private static StortingenModel buildStortingenModel(
            final String containerNr, final Long count, final LocalDate date,
            final String containerMeldingCategorie) {
        return StortingenModel.builder()
                .containerNummer(containerNr)
                .count(count)
                .date(date)
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
