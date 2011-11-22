package ch.unibe.scg.cc.activerecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.inject.Inject;

public class Function extends ActiveRecord {

	public Function(PreparedStatement insert) {
		super(insert);
	}

	int baseLine;
	String fname;
	String file_path;
	transient StringBuilder contents;
	
	public int getBaseLine() {
		return baseLine;
	}
	
	public void setBaseLine(int baseLine) {
		this.baseLine = baseLine;
	}
	public String getFname() {
		return fname;
	}
	public void setFname(String fname) {
		this.fname = fname;
	}
	public String getFile_path() {
		return file_path;
	}
	public void setFile_path(String file_path) {
		this.file_path = file_path;
	}

	public StringBuilder getContents() {
		return contents;
	}

	public void setContents(StringBuilder contents) {
		this.contents = contents;
	}

	public void setContents(String contents) {
		setContents(new StringBuilder(contents));
	}

	@Override
	protected void mySave() throws SQLException {
		insert.setInt(1, baseLine);
		insert.setString(2, fname);
		insert.setString(3, file_path);
	}

}
