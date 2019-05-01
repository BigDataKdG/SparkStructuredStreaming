import java.sql.Connection;
import java.sql.DriverManager;
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

    public static void main(String[] args) throws Exception {
        String jdbcUrl = "jdbc:postgresql://localhost:5432/sorteertstraatjes";
        String username = "postgres";
        String password = "admin";


        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("group.id", "test");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "model.ContainerMeldingSerializer");

        Producer<Integer, ContainerMelding> kafkaProducer =
                new org.apache.kafka.clients.producer.KafkaProducer(props);

        List<ContainerMelding> meldingenList = new ArrayList<>();

        try (Connection con = DriverManager.getConnection(jdbcUrl, username, password);
             Statement st = con.createStatement();

             ResultSet rs = st.executeQuery("SELECT * FROM \"public\".\"container\" limit 100")) {

            if (rs.next()) {
                System.out.println(rs.getString("datum_tijdstip_containeractiviteit"));
                meldingenList.add(ContainerMelding.builder()
                        .containerActiviteit(rs.getDate("datum_tijdstip_containeractiviteit").toLocalDate())
                        .containerNummer(rs.getInt("container_nr"))
                        .afvaltype(rs.getString("container_afvaltype"))
                        .containerMeldingCategorie(rs.getString("containermelding_categorie_code"))
                        .build());
            }
        } catch (SQLException ex) {

        }
        for (ContainerMelding melding : meldingenList) {
            kafkaProducer.send(new ProducerRecord(TOPIC, 0, melding));
        }
        kafkaProducer.close();
    }

}
