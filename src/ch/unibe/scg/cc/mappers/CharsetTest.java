package ch.unibe.scg.cc.mappers;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import ch.unibe.scg.cc.mappers.TablePopulator.CharsetDetector;

@SuppressWarnings("javadoc")
public final class CharsetTest {
	@Test
	public void testCharset() {
		CharsetDetector cd = new CharsetDetector();
		assertThat(cd.charsetOf(Bytes.toBytes("Hallö")), is("UTF-8"));
		assertThat(cd.charsetOf(Bytes.toBytes("Hallo")), is("ASCII"));
	}
}