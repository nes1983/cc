package ch.unibe.scg.cc.activerecord;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class Project extends Column {

	private static final String PROJECT_NAME = "projectName";
	String name;
	

	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		System.out.println(name);
		this.name = name;
	}
	
	public void save(Put put) {
        put.add(Bytes.toBytes("fact"), Bytes.toBytes(PROJECT_NAME), 0l, Bytes.toBytes(getName()));
	}
}
