package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;

import ch.unibe.scg.cc.StandardHasher;

public class Function extends Column {

	private CodeFile codeFile;
	int baseLine;
	String file_path;
	transient StringBuilder contents;

	@Inject
	StandardHasher standardHasher;

	public void save(Put put) throws IOException {
		// nothing to do
	}

	@Override
	public byte[] getHash() {
		return standardHasher.hash(getContents().toString());
	}

	public int getBaseLine() {
		return baseLine;
	}

	public void setBaseLine(int baseLine) {
		this.baseLine = baseLine;
	}

	public StringBuilder getContents() {
		return contents;
	}

	public void setContents(StringBuilder contents) {
		this.contents = contents;
	}

	public void setContents(String contents) {
		setContents(new StringBuilder(contents));
	}

	public void setCodeFile(CodeFile codeFile) {
		this.codeFile = codeFile;
	}

	public CodeFile getCodeFile() {
		return codeFile;
	}

}
