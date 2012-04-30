package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.inject.assistedinject.Assisted;

import ch.unibe.scg.cc.StandardHasher;
import ch.unibe.scg.cc.util.ByteUtils;

public class RealVersion extends Column implements Version {

	private static final String FILEPATH_NAME = "fp";

	private String filePath;
	private CodeFile codeFile;
	private byte[] hash;
	private byte[] hashFilePath;
	
	@Inject
	public RealVersion(StandardHasher standardHasher, @Assisted String filePath, @Assisted CodeFile codeFile) {
		this.filePath = filePath;
		this.codeFile = codeFile;
		this.hashFilePath = standardHasher.hash(getFilePath());
		byte[] hashVersion = ByteUtils.xor(codeFile.getFileContentsHash(), this.hashFilePath);
		this.hash = Bytes.add(hashVersion, codeFile.getFileContentsHash(), this.hashFilePath);
	}

	@Override
	public void save(Put put) throws IOException {
		put.add(Bytes.toBytes(FAMILY_NAME),  new byte[0], 0l, new byte[0]); //dummy add
        
        Put s = new Put(this.hashFilePath);
        s.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(FILEPATH_NAME), 0l, Bytes.toBytes(getFilePath()));
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
