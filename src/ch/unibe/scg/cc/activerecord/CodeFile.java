package ch.unibe.scg.cc.activerecord;

import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;

import ch.unibe.scg.cc.StandardHasher;

import com.google.common.collect.Lists;
import com.google.inject.assistedinject.Assisted;

public class CodeFile extends Column implements IColumn {
	public static final String FUNCTION_OFFSET_NAME = "fo";
	private final List<Function> functions;
	private final String fileContents;
	private final byte[] fileContentHash;

	@Inject
	public CodeFile(StandardHasher standardHasher, @Assisted String fileContents) {
		functions = Lists.newArrayList();
		this.fileContents = fileContents;
		this.fileContentHash = standardHasher.hash(getFileContents());
	}

	@Override
	public byte[] getHash() {
		return this.fileContentHash;
	}

	@Override
	public void save(Put put) throws IOException {
		throw new UnsupportedOperationException();
	}

	public String getFileContents() {
		return fileContents;
	}

	public void addFunction(Function function) {
		this.functions.add(function);
	}

	public List<Function> getFunctions() {
		return functions;
	}
}
