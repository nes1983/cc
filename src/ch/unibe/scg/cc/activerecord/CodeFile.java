package ch.unibe.scg.cc.activerecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import ch.unibe.scg.cc.StandardHasher;

public class CodeFile extends Column {

	public static final String FUNCTION_OFFSET_NAME = "fo";
	private List<Function> functions;
	private String fileContents;
	private byte[] fileContentHash;
	private boolean isOutdatedFileContentHash;

	@Inject
	StandardHasher standardHasher;

	public CodeFile() {
		functions = new ArrayList<Function>();
		isOutdatedFileContentHash = true;
	}

	@Override
	public byte[] getHash() {
		throw new NotImplementedException();
	}

	@Override
	public void save(Put put) throws IOException {
		throw new NotImplementedException();
	}

	public void setFileContents(String fileContents) {
		this.fileContents = fileContents;
		this.isOutdatedFileContentHash = true;
	}

	public String getFileContents() {
		return fileContents;
	}

	public byte[] getFileContentsHash() {
		if(isOutdatedFileContentHash) {
			this.fileContentHash = standardHasher.hash(getFileContents());
			this.isOutdatedFileContentHash = false;
		}
		return this.fileContentHash;
	}

	public void addFunction(Function function) {
		this.functions.add(function);
	}

	public List<Function> getFunctions() {
		return functions;
	}

}
