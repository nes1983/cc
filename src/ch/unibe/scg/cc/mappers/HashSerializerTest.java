package ch.unibe.scg.cc.mappers;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.primitives.UnsignedBytes;

public final class HashSerializerTest {
	@Test
	public void testDeserialize() {
		HashSerializer hs = new HashSerializer(UnsignedBytes.lexicographicalComparator());
		byte[] array = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7 };
		Set<byte[]> s = hs.deserialize(array, 2);
		byte[] b = hs.serialize(s);
		Assert.assertArrayEquals(array, b);
	}
}