package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.StandardHasher;


public class Project extends Column {

	private static final String PROJECT_NAME = "pn";
	String name;
	
	@Inject
	StandardHasher standardHasher;
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public void save(Put put) throws IOException {
        put.add(Bytes.toBytes("d"), Bytes.toBytes(PROJECT_NAME), 0l, standardHasher.hash(getName()));
        
        Put stringPut = new Put(standardHasher.hash(getName()));
        stringPut.add(Bytes.toBytes("d"), Bytes.toBytes(PROJECT_NAME), 0l, Bytes.toBytes(getName()));
        strings.put(stringPut);
	}
}
