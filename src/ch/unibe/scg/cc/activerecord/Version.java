package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.StandardHasher;

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
		this.hash = xor(this.hashCodeFile, this.hashFilePath);
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

	/**
	 * Compute the bitwise XOR of two arrays of bytes. The arrays have to be of same length.
	 *
	 * @return {@code x1} XOR {@code x2}
	 */
	static byte[] xor(byte[] x1, byte[] x2) {
		assert x1.length == x2.length;
		byte[] out = new byte[x1.length];

		for (int i = out.length - 1; i >= 0; i--) {
			out[i] = (byte) (x1[i] ^ x2[i]);
		}
		return out;
	}
}
