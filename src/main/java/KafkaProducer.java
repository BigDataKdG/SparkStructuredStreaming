import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
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
        while (true) {
            sendToKafka(retrieveMeldingenFromDatabase(counter), kafkaProducer);
            Thread.sleep(10000);
            counter += 50;
        }
    }

    private static List<ContainerMelding> retrieveMeldingenFromDatabase(int counter) throws SQLException {
        List<ContainerMelding> meldingenList = new ArrayList<>();
        String sql =
                "SELECT * FROM public.container LIMIT ? OFFSET ?";
        Connection con = DriverManager.getConnection(jdbcUrl, username, password);
        PreparedStatement st = con.prepareStatement(sql);
        st.setInt(1, 50);
        st.setInt(2, counter);
        ResultSet rs = st.executeQuery();

        try {
            while (rs.next()) {
                meldingenList.add(ContainerMelding.builder()
                        .containerActiviteit(rs.getDate("datum_tijdstip_containeractiviteit").toLocalDate())
                        .containerNummer(rs.getInt("container_nr"))
                        .containerMeldingCategorie(rs.getString("containermelding_categorie_code"))
                        .build());
            }
        } catch (Exception ex) {

        }
        return meldingenList;
    }

    private static void sendToKafka(List<ContainerMelding> meldingenList,
                                    final Producer<Integer, ContainerMelding> kafkaProducer) {
        for (ContainerMelding melding : meldingenList) {
            System.out.println(melding);
            kafkaProducer.send(new ProducerRecord(TOPIC, 0, melding));

        }
    }
}
