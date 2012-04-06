package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.StandardHasher;

import com.google.inject.assistedinject.Assisted;


public class RealProject extends Column implements Project {

	private static final String PROJECT_NAME = "pn";
	private static final String VERSIONNUMBER_NAME = "vn";
	private String name;
	private Version version;
	private int versionNumber;
	private byte[] hash;
	private byte[] hashName;
	
	@Inject
	public RealProject(StandardHasher standardHasher, @Assisted String name, @Assisted Version version, @Assisted int versionNumber) {
		this.name = name;
		this.version = version;
		this.versionNumber = versionNumber;
		this.hashName = standardHasher.hash(getName());
		this.hash = Bytes.add(hashName, version.getHash());
	}

	public void save(Put put) throws IOException {
		put.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(VERSIONNUMBER_NAME), 0l, Bytes.toBytes(versionNumber));
        
        Put s = new Put(this.hashName);
        s.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(PROJECT_NAME), 0l, Bytes.toBytes(getName()));
        strings.put(s);
	}

	@Override
	public byte[] getHash() {
		return this.hash;
	}
	
	public String getName() {
		return name;
	}
	
	public Version getVersion() {
		return version;
	}

	public int getVersionNumber() {
		return versionNumber;
	}
}
