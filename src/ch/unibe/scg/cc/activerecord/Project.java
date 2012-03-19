package ch.unibe.scg.cc.activerecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.StandardHasher;


public class Project extends Column {

	private static final String PROJECT_NAME = "pn";
	private static final String PROJECTVERSION_NAME = "pv";
	private static final String FILEPATH_NAME = "fp";
	String name;
	String version;
	String filePath;
	byte[] hash;
	boolean isOutdatedHash;
	
	@Inject
	StandardHasher standardHasher;
	
	public Project() {
		this.isOutdatedHash = true;
	}

	public void save(Put put) throws IOException {
		byte[] hashName = standardHasher.hash(getName());
		byte[] hashVersion = standardHasher.hash(getVersion());
		byte[] hashFilePath = standardHasher.hash(getFilePath());

		put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(PROJECT_NAME), 0l, hashName);
		put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(PROJECTVERSION_NAME), 0l, hashVersion);
		put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(FILEPATH_NAME), 0l, hashFilePath);
        
        List<Put> stringPuts = new ArrayList<Put>();
        Put s1 = new Put(hashName);
        s1.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(PROJECT_NAME), 0l, Bytes.toBytes(getName()));
        Put s2 = new Put(hashVersion);
        s2.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(PROJECTVERSION_NAME), 0l, Bytes.toBytes(getVersion()));
        Put s3 = new Put(hashFilePath);
        s3.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(FILEPATH_NAME), 0l, Bytes.toBytes(getFilePath()));
        
        stringPuts.add(s1);
        stringPuts.add(s2);
        stringPuts.add(s3);
        strings.put(stringPuts);
	}

	@Override
	public byte[] getHash() {
		assert name != null;
		assert version != null;
		assert filePath != null;
		if(this.isOutdatedHash) {
			this.hash = standardHasher.hash(new String(Bytes.add(standardHasher.hash(getName()), standardHasher.hash(getVersion()), standardHasher.hash(getFilePath())))); // XXX efficient??
			this.isOutdatedHash = false;
		}
		return this.hash;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
		this.isOutdatedHash = true;
	}
	
	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
		this.isOutdatedHash = true;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
		this.isOutdatedHash = true;
	}

	public String getFilePath() {
		return filePath;
	}
}
