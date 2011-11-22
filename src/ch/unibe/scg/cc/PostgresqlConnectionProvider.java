package ch.unibe.scg.cc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.google.inject.Provider;

public class PostgresqlConnectionProvider implements Provider<Connection> {
	
	void loadDriver() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Could not load the Database Driver", e);
		}
	}

	@Override
	public Connection get() {
		loadDriver();

		Properties props = new Properties();
		props.setProperty("user","niko");
		props.setProperty("password","Richardigel");
		props.setProperty("tcpKeepAlive", "true");
		props.setProperty("prepareThreshold", "1");
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(
					"jdbc:postgresql://pinocchio/cc_ref", props);
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			throw new RuntimeException("Could not connect to database.", e);
		}
		return conn;
	}
}