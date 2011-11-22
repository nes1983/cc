package ch.unibe.scg.cc.activerecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.inject.Provider;

@Singleton
public class FunctionProvider implements Provider<Function> {
	final static String INSERT = "INSERT INTO function (function, base_line, fname, file_path) VALUES (DEFAULT, ?, ?, ?) RETURNING function";
	
	final PreparedStatement functionInsert;
	
	@Inject
	public FunctionProvider(Connection connection) {
		try {
			functionInsert = connection.prepareStatement(INSERT);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Function get() {
		return new Function(functionInsert);
	}

}
