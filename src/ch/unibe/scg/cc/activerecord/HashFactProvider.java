package ch.unibe.scg.cc.activerecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.inject.Provider;

@Singleton
public class HashFactProvider implements Provider<HashFact> {
	private final static String INSERT = "INSERT INTO hashfact (hashfact, hash, project, function, from_line, length, type) VALUES (DEFAULT, ?, ?, ?, ?, ?, ?)";
	

	final PreparedStatement insert;
	
	@Inject
	public HashFactProvider(Connection connection) {
		try {
			insert = connection.prepareStatement(INSERT);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public HashFact get() {
		return new HashFact(insert);
	}

}
