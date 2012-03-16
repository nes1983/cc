package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.StandardHasher;


public class CodeFile extends Column {

	private Project project;
	private String fileContents;
	
	@Inject
	StandardHasher standardHasher;

	@Override
	public byte[] getHash() {
		assert fileContents != null;
		assert project != null;
		return Bytes.add(standardHasher.hash(getFileContents()), project.getHash());
	}

	@Override
	public void save(Put put) throws IOException {
		put.add(Bytes.toBytes(FAMILY_NAME),  new byte[0], 0l, new byte[0]); //dummy add
	}

	public void setFileContents(String fileContents) {
		this.fileContents = fileContents;
	}

	public String getFileContents() {
		return fileContents;
	}

	public void setProject(Project project) {
		this.project = project;
	}

	public Project getProject() {
		return project;
	}
	
}
