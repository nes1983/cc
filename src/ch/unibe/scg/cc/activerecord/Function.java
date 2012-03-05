package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.StandardHasher;

public class Function extends Column {

	private static final String FILEPATH = "fp";
	int baseLine;
	String file_path;
	transient StringBuilder contents;
	
	@Inject
	StandardHasher standardHasher;
	
	public int getBaseLine() {
		return baseLine;
	}
	
	public void setBaseLine(int baseLine) {
		this.baseLine = baseLine;
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
	
	public void save(Put put) throws IOException {
        put.add(Bytes.toBytes("d"), Bytes.toBytes(FILEPATH), 0l, standardHasher.hash(getFile_path()));
        
        Put stringPut = new Put(standardHasher.hash(getFile_path()));
        stringPut.add(Bytes.toBytes("d"), Bytes.toBytes(FILEPATH), 0l, Bytes.toBytes(getFile_path()));
        strings.put(stringPut);
	}

}
