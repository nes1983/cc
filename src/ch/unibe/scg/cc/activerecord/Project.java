package ch.unibe.scg.cc.activerecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.inject.Inject;


public class Project extends ActiveRecord {

	public Project(PreparedStatement insert) {
		super(insert);
	}

	String name;
	

	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		System.out.println(name);
		this.name = name;
	}
	
	@Override
	protected void mySave() throws SQLException {
		insert.setString(1, name);
	}
}
