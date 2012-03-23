package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.StandardHasher;


public class Project extends Column {

	private static final String PROJECT_NAME = "pn";
	private static final String VERSIONNUMBER_NAME = "vn";
	private String name;
	private Version version;
	private int versionNumber;
	private byte[] hash;
	private boolean isOutdatedHash;
	
	@Inject
	StandardHasher standardHasher;
	
	public Project() {
		this.isOutdatedHash = true;
	}

	public void save(Put put) throws IOException {
		byte[] hashName = standardHasher.hash(getName());

		put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(VERSIONNUMBER_NAME), 0l, Bytes.toBytes(versionNumber));
        
        Put s = new Put(hashName);
        s.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(PROJECT_NAME), 0l, Bytes.toBytes(getName()));
        strings.put(s);
	}

	@Override
	public byte[] getHash() {
		assert getName() != null;
		assert getVersion() != null;
		if(this.isOutdatedHash) {
			this.hash = Bytes.add(standardHasher.hash(getName()), version.getHash());
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
	
	public Version getVersion() {
		return version;
	}

	public void setVersion(Version version) {
		this.version = version;
		this.isOutdatedHash = true;
	}

	public int getVersionNumber() {
		return versionNumber;
	}

	public void setVersionNumber(int versionNumber) {
		this.versionNumber = versionNumber;
	}
}
