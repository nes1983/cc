package org.unibe.scg.cells;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

@SuppressWarnings("javadoc")
public final class InMemoryShufflerTest {
	@Test
	public void testReadRow() {
		try (InMemoryShuffler<Byte> s = new InMemoryShuffler<>()) {
			s.write(new Cell<Byte>(ByteString.copyFromUtf8("aaab"), ByteString.EMPTY, ByteString.EMPTY));
			s.write(new Cell<Byte>(ByteString.copyFromUtf8("aaac"), ByteString.EMPTY, ByteString.EMPTY));
			Cell<Byte> aaad0 =
					new Cell<>(ByteString.copyFromUtf8("aaad"), ByteString.copyFromUtf8("0"), ByteString.EMPTY);
			s.write(aaad0);
			Cell<Byte> aaad1 =
					new Cell<>(ByteString.copyFromUtf8("aaad"), ByteString.copyFromUtf8("1"), ByteString.EMPTY);
			s.write(aaad1);
			s.close();

			assertThat(Iterables.getOnlyElement(s.readRow(ByteString.copyFromUtf8("aaab"))).getRowKey().toStringUtf8(),
					is("aaab"));
			assertThat(Iterables.getOnlyElement(s.readRow(ByteString.copyFromUtf8("aaac"))).getRowKey().toStringUtf8(),
					is("aaac"));
			Iterable<Cell<Byte>> aaad = s.readRow(ByteString.copyFromUtf8("aaad"));
			assertThat(aaad.toString(), Iterables.size(aaad), is(2));
			assertThat(Iterables.get(aaad, 0), is(aaad0));
			assertThat(Iterables.get(aaad, 1), is(aaad1));

			assertTrue(Iterables.isEmpty(s.readRow(ByteString.copyFromUtf8("aaaa"))));
			assertTrue(Iterables.isEmpty(s.readRow(ByteString.copyFromUtf8("aaae"))));
		}
	}
}
