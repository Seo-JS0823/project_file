package insert_data.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Access {
	private String driver = "oracle.jdbc.driver.OracleDriver";
	private String url = "jdbc:oracle:thin:@localhost:1521:xe";
	private String name = "project1";
	private String pass = "1234";
	
	public Connection getConnection() throws SQLException, ClassNotFoundException {
		Class.forName(driver);
		Connection con = DriverManager.getConnection(url, name, pass);
		
		return con;
	}
}
