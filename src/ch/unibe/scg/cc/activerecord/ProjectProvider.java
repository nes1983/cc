package ch.unibe.scg.cc.activerecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.inject.Provider;

@Singleton
public class ProjectProvider implements Provider<Project> {
	final static String INSERT = "INSERT INTO project (project, name) VALUES (DEFAULT, ?) RETURNING project";
	
	final PreparedStatement insert;
	
	@Inject
	public ProjectProvider(Connection connection) {
		try {
			insert = connection.prepareStatement(INSERT);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Project get() {
		return new Project(insert);
	}

}
