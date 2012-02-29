package ch.unibe.scg.cc.activerecord;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Function extends Column {

	private static final String FILEPATH = "filePath";
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
	
	public void save(Put put) {
        put.add(Bytes.toBytes("f1"), Bytes.toBytes(FILEPATH), 0l, Bytes.toBytes(getFile_path()));
	}

}
