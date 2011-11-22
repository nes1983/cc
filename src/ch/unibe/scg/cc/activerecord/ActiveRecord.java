package ch.unibe.scg.cc.activerecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.inject.Inject;

public abstract class ActiveRecord implements Cloneable {
	
	final protected PreparedStatement insert;
	
	protected Long id;
	
	public long getId() {
		if(id == null)
			throw new RuntimeException("You have to save me first");
		return id;
	}
	
	protected void getKey(ResultSet key) throws SQLException {
		boolean keyGenerated = key.next();
		assert keyGenerated == true;
		id = key.getLong(1);
	}
	
	@Inject
	public ActiveRecord(PreparedStatement insert) {
		this.insert = insert;
	}
	
	public void save() throws SQLException  {
		if(id != null)
			return;
		mySave();
		execute();
		assert id != null;
	}
	protected abstract  void mySave() throws SQLException;
	 

	protected void execute() throws SQLException {
		ResultSet key = insert.executeQuery();
		getKey(key);
	}
	
	public Object clone()  {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
	
}
