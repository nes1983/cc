package ch.unibe.scg.cc.activerecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.inject.Inject;


public class Location extends ActiveRecord {

	public Location(PreparedStatement insert)  {
		super(insert);
	}



	int length;
	int firstLine;


	
	@Override
	protected void mySave() throws SQLException {
		insert.setInt(1, firstLine);
		insert.setInt(2, length);
	}
	
	public int getLength() {
		return length;
	}


	public void setLength(int length) {
		this.length = length;
	}


	public int getFirstLine() {
		return firstLine;
	}


	public void setFirstLine(int firstLine) {
		this.firstLine = firstLine;
	}
}
