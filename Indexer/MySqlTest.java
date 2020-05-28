
 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
 
 
public class MySqlTest {
 
    final static String Db_name = "testdb";
    final static String url1 = "jdbc:mysql://localhost:3306/" + Db_name;
    final static String user = "jimmy";
    final static String password = "jimmy";
    private static Connection c = null;
    private static Statement stmt = null;
 
    public static void connect() {
        try {
 
            c = DriverManager.getConnection(url1, user, password);
            if (c != null) {
                System.out.println("Connected to the database " + Db_name);
            }
 
        } catch (SQLException ex) {
            System.out.println("An error occurred. Maybe user/password is invalid");
            ex.printStackTrace();
        }
    }
 
    public static void print(Object s) {
        System.out.println(s);
    }
 
    public static void Insert_Author(String name) {
        try {
 
            String sql = "INSERT INTO Authors(Name) VALUES(?)";
 
            PreparedStatement pst = c.prepareStatement(sql); // prepare insert query
 
            pst.setString(1, name);
            int done = pst.executeUpdate();
 
            if (done > 0) {
                print("Insert Record successfully");
            }
 
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
 
    }
 
    public static void Select_All() {
        try {
 
            String sql = "Select * from Authors ;";
 
            PreparedStatement pst = c.prepareStatement(sql); // prepare select * query
 
            ResultSet rs = pst.executeQuery();
 
            while (rs.next()) {
 
                System.out.print(rs.getInt(1));
                System.out.print(": ");
                System.out.println(rs.getString(2));
            }
 
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
 
    }
 
    public static void main(String[] args) {
 
        connect();
        String author = "Medhat";
 
        Insert_Author(author);
        Select_All();
    }
 
}