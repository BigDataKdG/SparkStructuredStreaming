import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import model.ContainerMelding;


public class KafkaProducer {
    private final static String GROUP_ID = "sorteerstraatjes";
    private final static String STORTINGEN_TOPIC = "stortingen";
    private final static String LEDIGINGEN_TOPIC = "ledigingen";

    private final static String BOOTSTRAP_SERVER = "localhost:9092";
    private final static String jdbcUrl = "jdbc:postgresql://localhost:5432/";


    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("group.id", GROUP_ID);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "model.ContainerMeldingSerializer");

        Producer<Integer, ContainerMelding> kafkaProducer =
                new org.apache.kafka.clients.producer.KafkaProducer(props);

        sendToKafka(getContainerMeldingen("ledigingen"), kafkaProducer, LEDIGINGEN_TOPIC);
        sendToKafka(getContainerMeldingen("stortingen"), kafkaProducer, STORTINGEN_TOPIC);
    }

    private static List<ContainerMelding> getContainerMeldingen(final String typeMelding) throws Exception {
        Connection con = DriverManager.getConnection(jdbcUrl + System.getenv("DATABASE_NAME"), System.getenv(
                "USERNAME"), System.getenv("PASSWORD"));

        String query = getQuery(typeMelding);

        PreparedStatement st = con.prepareStatement(query);
        ResultSet rs = st.executeQuery();

        List<ContainerMelding> containerMeldingen = new ArrayList<>();

        try {
            while (rs.next()) {
                containerMeldingen.add(ContainerMelding.builder()
                        .containerActiviteit(
                                rs.getTimestamp("datum_tijdstip_containeractiviteit").toLocalDateTime())
                        .containerNummer(rs.getInt("container_nr"))
                        .dayOfWeek(rs.getTimestamp("datum_tijdstip_containeractiviteit")
                                .toLocalDateTime().getDayOfWeek().getValue())
                        .containerMeldingId(rs.getInt("containermelding_id"))
                        .build());
            }
        } catch (Exception ex) {

        }
        return containerMeldingen;
    }

    private static String getQuery(final String typeMelding) {
        if (typeMelding.equals("stortingen")) {
            return "SELECT distinct * FROM public.container WHERE to_date(SPLIT_PART(public.container"
                    + ".datum_tijdstip_containeractiviteit, ' ',1), 'YYYY/MM/DD') BETWEEN"
                    + "'2018/08/01' AND '2018/08/31' AND (containermelding_id = '20' or containermelding_id = "
                    + "'21') AND (container_nr = '255' or container_nr = '357' or "
                    + "container_nr = '599') order by 1 asc";
        }
        return "SELECT distinct * FROM public.container WHERE to_date(SPLIT_PART(public.container"
                + ".datum_tijdstip_containeractiviteit, ' ', 1), 'YYYY/MM/DD') BETWEEN '2018/08/01' AND "
                + "'2018/08/31'"
                + "AND (containermelding_id = '11' OR containermelding_id = '77') AND  ("
                + "container_nr = '255' or container_nr = '357' or container_nr = '599') order by 1  asc";
    }

    private static void sendToKafka(
            List<ContainerMelding> meldingenList,
            final Producer<Integer, ContainerMelding> kafkaProducer,
            String topic) throws Exception {
        for (ContainerMelding melding : meldingenList) {
            System.out.println(melding);

            kafkaProducer.send(new ProducerRecord(topic, 0, melding));
        }
    }
}
