package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.StandardHasher;
import ch.unibe.scg.cc.mappers.ByteUtils;

import com.google.inject.assistedinject.Assisted;

public class Version extends Column implements IColumn {
	private static final byte[] FILEPATH_NAME = Bytes.toBytes("fp");

	final String filePath;
	final byte[] hash;
	final byte[] hashCodeFile;
	final byte[] hashFilePath;
	final PutFactory putFactory;

	@Inject
	public Version(StandardHasher standardHasher, @Assisted String filePath, @Assisted CodeFile codeFile,
			PutFactory putFactory) {
		this.filePath = filePath;
		this.hashCodeFile = codeFile.getHash();
		this.hashFilePath = standardHasher.hash(filePath);
		this.hash = ByteUtils.xor(this.hashCodeFile, this.hashFilePath);
		this.putFactory = putFactory;
	}

	@Override
	public void save(Put put) throws IOException {
		put.add(FAMILY_NAME, this.hashCodeFile, 0l, this.hashFilePath);

		Put s = putFactory.create(this.hashFilePath);
		s.add(FAMILY_NAME, FILEPATH_NAME, 0l, Bytes.toBytes(filePath));
		strings.put(s);
	}

	@Override
	public byte[] getHash() {
		return this.hash;
	}
}
