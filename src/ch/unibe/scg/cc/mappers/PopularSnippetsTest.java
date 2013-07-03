package ch.unibe.scg.cc.mappers;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import ch.unibe.scg.cc.Protos.Snippet;

import com.google.protobuf.ByteString;


@SuppressWarnings("javadoc")
public final class PopularSnippetsTest {
	@Test
	public void testInShouldBeOut() {
		Snippet loc = Snippet
				.newBuilder()
				.setPosition(10)
				.setLength(5)
				.setFunction(
						ByteString.copyFrom(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
								19, 20 }))
				.setHash(
						ByteString.copyFrom(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
								19, 20, 21 })).build();

		byte[] rowKey = PopularSnippetsCodec.encodeRowKey(loc);
		byte[] colKey = PopularSnippetsCodec.encodeColumnKey(loc);
		byte[] cellVal = PopularSnippetsCodec.encodeCellValue(loc);

		assertThat(PopularSnippetsCodec.decodeSnippetLocation(rowKey, colKey, cellVal), is(loc));
	}
}
