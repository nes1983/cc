package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.StandardHasher;

public class HashFact extends Column {

	private static final String LOCATION_FIRST_LINE_NAME = "fl";
	private static final String LOCATION_LENGTH_NAME = "ll";

	private byte[] hash;
	private Project project;
	private Function function;
	private Location location;
	private int type;
	
	StandardHasher standardHasher;

	@Inject
	public HashFact(StandardHasher standardHasher) {
		this.standardHasher = standardHasher;
	}

	@Override
	public void save(Put put) throws IOException {
		assert location != null;
    	put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(LOCATION_FIRST_LINE_NAME), 0l, Bytes.toBytes(location.getFirstLine()));
    	put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(LOCATION_LENGTH_NAME), 0l, Bytes.toBytes(location.getLength()));
	}

	public byte[] getHash() {
		return Bytes.add(new byte[]{(byte) type}, hash);
	}

	public void setHash(byte[] hash) {
		assert hash.length == 20;
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
