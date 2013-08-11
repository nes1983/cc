package ch.unibe.scg.cc;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.google.common.base.Charsets;


@SuppressWarnings("javadoc")
public final class CharsetTest {
	@Test
	public void testCharset() {
		CharsetDetector cd = new CharsetDetector();
		simpleTest(cd);
	}

	private void simpleTest(CharsetDetector cd) {
		assertThat(cd.charsetOf(Bytes.toBytes("Hallö")), is(Charsets.UTF_8));
		assertThat(cd.charsetOf(Bytes.toBytes("Hallo")), is(Charsets.US_ASCII));
	}

	@Test
	public void testSerialize() throws IOException, ClassNotFoundException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = new ObjectOutputStream(bos);
		out.writeObject(new CharsetDetector());
		byte[] yourBytes = bos.toByteArray();

		CharsetDetector copy = (CharsetDetector) new ObjectInputStream(new ByteArrayInputStream(yourBytes)).readObject();
		simpleTest(copy);
	}
}