package ch.unibe.scg.cc.activerecord;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.inject.Inject;

import ch.unibe.scg.cc.lines.StringOfLines;

import postmatchphase.HashFactLoader;


public class HashFact extends ActiveRecord {
	
	public HashFact(PreparedStatement insert) {
		super(insert);
	}

	byte[] hash;
	Project project;
	Function function;
	Location location;
	int type;

//	/**
//	 * Loads the entire contents of the file referenced by this fact, and not just the snippet it references.
//	 * @param loader
//	 * @return
//	 * @throws IOException
//	 */
//	public String loadFrom(HashFactLoader loader) throws IOException {
//		return loader.loadFile(this); 
//	}

	@Override
	protected void mySave() throws SQLException {
		project.save();
		//location.save();
		function.save();
		insert.setBytes(1, hash);
		insert.setLong(2, project.getId());
		insert.setLong(3, function.getId());
		//insert.setLong(4, location.getId());
		insert.setInt(4, location.getFirstLine());
		insert.setInt(5, location.getLength());
		insert.setInt(6, type);
	}
	
	@Override 
	public void save() throws SQLException  {
		mySave();
		execute();
	}
	
	@Override
	protected void execute() throws SQLException {
		insert.executeUpdate();
	}

	public byte[] getHash() {
		return hash;
	}

	public void setHash(byte[] hash) {
		this.hash = hash;
	}

	public Project getProject() {
		return project;
	}

	public void setProject(Project project) {
		this.project = project;
	}

	public Location getLocation() {
		return location;
	}

	public void setLocation(Location location) {
		this.location = location;
	}
	
	public Function getFunction() {
		return function;
	}

	public void setFunction(Function function) {
		this.function = function;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}


}
