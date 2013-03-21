package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.ByteUtils;
import ch.unibe.scg.cc.StandardHasher;

import com.google.inject.assistedinject.Assisted;

public class RealVersion extends Column implements Version {

	private static final byte[] FILEPATH_NAME = Bytes.toBytes("fp");

	private String filePath;
	private CodeFile codeFile;
	private byte[] hash;
	private byte[] hashCodeFile;
	private byte[] hashFilePath;

	@Inject
	public RealVersion(StandardHasher standardHasher, @Assisted String filePath, @Assisted CodeFile codeFile) {
		this.filePath = filePath;
		this.codeFile = codeFile;
		this.hashCodeFile = codeFile.getFileContentsHash();
		this.hashFilePath = standardHasher.hash(getFilePath());
		this.hash = ByteUtils.xor(this.hashCodeFile, this.hashFilePath);
	}

	@Override
	public void save(Put put) throws IOException {
		put.add(FAMILY_NAME, this.hashCodeFile, 0l, this.hashFilePath);

		Put s = new Put(this.hashFilePath);
		s.add(FAMILY_NAME, FILEPATH_NAME, 0l, Bytes.toBytes(getFilePath()));
		strings.put(s);
	}

	@Override
	public byte[] getHash() {
		return this.hash;
	}

	public String getFilePath() {
		return filePath;
	}

	public CodeFile getCodeFile() {
		return codeFile;
	}

}
