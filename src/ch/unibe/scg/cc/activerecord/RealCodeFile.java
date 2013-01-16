package ch.unibe.scg.cc.activerecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;

import com.google.inject.assistedinject.Assisted;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import ch.unibe.scg.cc.StandardHasher;

public class RealCodeFile extends Column implements CodeFile {

	public static final String FUNCTION_OFFSET_NAME = "fo";
	private List<Function> functions;
	private String fileContents;
	private byte[] fileContentHash;

	@Inject
	public RealCodeFile(StandardHasher standardHasher, @Assisted String fileContents) {
		functions = new ArrayList<Function>();
		this.fileContents = fileContents;
		this.fileContentHash = standardHasher.hash(getFileContents());
	}

	@Override
	public byte[] getHash() {
		throw new NotImplementedException();
	}

	@Override
	public void save(Put put) throws IOException {
		throw new NotImplementedException();
	}

	public String getFileContents() {
		return fileContents;
	}

	public byte[] getFileContentsHash() {
		return this.fileContentHash;
	}

	public void addFunction(Function function) {
		this.functions.add(function);
	}

	public List<Function> getFunctions() {
		return functions;
	}

}
