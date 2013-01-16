package ch.unibe.scg.cc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.google.inject.Provider;

public class VerticaConnectionProvider implements Provider<Connection> {

	void loadDriver() {
		try {
			Class.forName("com.vertica.Driver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Could not load the Database Driver", e);
		}
	}

	@Override
	public Connection get() {
		loadDriver();

		Properties myProp = new Properties();
		myProp.put("user", "dbadmin");
		Connection conn = null;
		try {
			conn = DriverManager.getConnection("jdbc:vertica://[fe80::5ef3:fcff:fe78:8ef0]/cc", myProp);
		} catch (SQLException e) {
			throw new RuntimeException("Could not connect to database.", e);
		}
		return conn;
	}
}
