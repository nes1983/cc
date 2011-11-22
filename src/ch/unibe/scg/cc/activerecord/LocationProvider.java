package ch.unibe.scg.cc.activerecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.inject.Provider;

@Singleton
public class LocationProvider implements Provider<Location> {
	final private static String INSERT = "INSERT INTO location (location, first_line, length) VALUES (DEFAULT, ?, ?) RETURNING location";

	final PreparedStatement insert;
	
	@Inject
	public LocationProvider(Connection connection) {
		try {
			insert = connection.prepareStatement(INSERT);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Location get() {
		return new Location(insert);
	}

}
