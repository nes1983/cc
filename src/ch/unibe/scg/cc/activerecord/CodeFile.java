package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.StandardHasher;


public class CodeFile extends Column {

	private Project project;
	private String fileContents;
	private byte[] hash;
	private boolean isOutdatedHash;
	
	@Inject
	StandardHasher standardHasher;
	
	public CodeFile() {
		isOutdatedHash = true;
	}

	@Override
	public byte[] getHash() {
		assert fileContents != null;
		assert project != null;
		if(this.isOutdatedHash) {
			hash = Bytes.add(standardHasher.hash(getFileContents()), project.getHash());
			this.isOutdatedHash = false;
		}
		return hash;
	}

	@Override
	public void save(Put put) throws IOException {
		put.add(Bytes.toBytes(FAMILY_NAME),  new byte[0], 0l, new byte[0]); //dummy add
	}

	public void setFileContents(String fileContents) {
		this.fileContents = fileContents;
		this.isOutdatedHash = true;
	}

	public String getFileContents() {
		return fileContents;
	}

	public void setProject(Project project) {
		this.project = project;
		this.isOutdatedHash = true;
	}

	public Project getProject() {
		return project;
	}
	
}
