import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class KafkaProducer {
    public static void main(String[] args) {
        // todo: fix typo in sorteer*T*straatjes db name
        String jdbcUrl = "jdbc:postgresql://localhost:5432/sorteertstraatjes";
        String username = "postgres";
        String password = "admin";

        try (Connection con = DriverManager.getConnection(jdbcUrl, username, password);
             Statement st = con.createStatement();
             ResultSet rs = st.executeQuery("SELECT * FROM \"default\".\"Container\"")) {
            int i = 1;
            if (rs.next()) {
                System.out.println(rs.getString(i++));
            }

        } catch (SQLException ex) {

        }
    }
}
