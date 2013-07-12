package ch.unibe.scg.cc;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.google.common.base.Charsets;


@SuppressWarnings("javadoc")
public final class CharsetTest {
	@Test
	public void testCharset() {
		CharsetDetector cd = new CharsetDetector();
		assertThat(cd.charsetOf(Bytes.toBytes("Hallö")), is(Charsets.UTF_8));
		assertThat(cd.charsetOf(Bytes.toBytes("Hallo")), is(Charsets.US_ASCII));
	}
}