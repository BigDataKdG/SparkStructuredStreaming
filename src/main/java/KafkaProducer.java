import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import model.ContainerMelding;


public class KafkaProducer {
    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVER = "localhost:9092";
    private final static String jdbcUrl = "jdbc:postgresql://localhost:5432/sorteertstraatjes";
    private final static String username = "postgres";
    private final static String password = "admin";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("group.id", "test");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "model.ContainerMeldingSerializer");

        Producer<Integer, ContainerMelding> kafkaProducer =
                new org.apache.kafka.clients.producer.KafkaProducer(props);
        int counter = 0;
        sendToKafka(retrieveMeldingenFromDatabase(counter), kafkaProducer);
        //Thread.sleep(1000);
        //counter += 50;

    }

    private static List<ContainerMelding> retrieveMeldingenFromDatabase(int counter) throws SQLException {
        List<ContainerMelding> meldingenList = new ArrayList<>();
        String stortingen =
                "SELECT distinct * FROM public.container WHERE to_date(SPLIT_PART(public.container"
                        + ".datum_tijdstip_containeractiviteit, ' ',1), 'YYYY/MM/DD') BETWEEN"
        + "'2018/03/13' AND '2018/03/27' AND (containermelding_id = '20' AND container_nr = '466')";

        String ledigingen =
                "SELECT distinct * FROM public.container WHERE to_date(SPLIT_PART(public.container.datum_tijdstip_containeractiviteit, ' ', 1), 'YYYY/MM/DD') BETWEEN '2018/03/01' AND '2018/03/30'"
        + "AND (containermelding_id = '11' OR containermelding_id = '77') AND container_nr = '466'";

        Connection con = DriverManager.getConnection(jdbcUrl, username, password);
        PreparedStatement st = con.prepareStatement(stortingen);
        PreparedStatement st2 = con.prepareStatement(ledigingen);
        //st.setInt(1, 50);
        //st.setInt(2, counter);
        ResultSet rs = st.executeQuery();
        ResultSet rs2 = st2.executeQuery();

        try {
            while (rs.next()) {
                meldingenList.add(ContainerMelding.builder()
                        .containerActiviteit(rs.getTimestamp("datum_tijdstip_containeractiviteit").toLocalDateTime())
                        .containerNummer(rs.getInt("container_nr"))
                        .dayOfWeek(rs.getTimestamp("datum_tijdstip_containeractiviteit").toLocalDateTime().getDayOfWeek().getValue())
                        .containerMeldingId(rs.getInt("containermelding_id"))
                        .build());
            }
        } catch (Exception ex) {

        }

        try {
            while (rs2.next()) {
                meldingenList.add(ContainerMelding.builder()
                        .containerActiviteit(rs2.getTimestamp("datum_tijdstip_containeractiviteit").toLocalDateTime())
                        .containerNummer(rs2.getInt("container_nr"))
                        .dayOfWeek(rs2.getTimestamp("datum_tijdstip_containeractiviteit").toLocalDateTime().getDayOfWeek().getValue())
                        .containerMeldingId(rs2.getInt("containermelding_id"))
                        .build());
            }
        } catch (Exception ex) {

        }

        return meldingenList;
    }

    private static void sendToKafka(
            List<ContainerMelding> meldingenList,
            final Producer<Integer, ContainerMelding> kafkaProducer) {

        for (ContainerMelding melding : meldingenList) {
            System.out.println(melding);
            kafkaProducer.send(new ProducerRecord(TOPIC, 0, melding));
        }
    }
}
