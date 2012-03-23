package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.StandardHasher;
import ch.unibe.scg.cc.util.ByteUtils;

public class Version extends Column {

	private static final String FILEPATH_NAME = "fp";

	private String filePath;
	private CodeFile codeFile;
	private byte[] hash;
	private boolean isOutdatedHash;
	
	@Inject
	StandardHasher standardHasher;
	
	public Version() {
		this.isOutdatedHash = true;
	}

	@Override
	public void save(Put put) throws IOException {
		byte[] hashFilePath = standardHasher.hash(getFilePath());

		put.add(Bytes.toBytes(FAMILY_NAME),  new byte[0], 0l, new byte[0]); //dummy add
        
        Put s = new Put(hashFilePath);
        s.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(FILEPATH_NAME), 0l, Bytes.toBytes(getFilePath()));
        strings.put(s);
	}

	@Override
	public byte[] getHash() {
		assert filePath != null;
		assert codeFile != null;
		if(this.isOutdatedHash) {
			this.hash = ByteUtils.xor(codeFile.getFileContentsHash(), standardHasher.hash(getFilePath()));
			this.isOutdatedHash = false;
		}
		return this.hash;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
		this.isOutdatedHash = true;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setCodeFile(CodeFile codeFile) {
		this.codeFile = codeFile;
		this.isOutdatedHash = true;
	}

	public CodeFile getCodeFile() {
		return codeFile;
	}

}
