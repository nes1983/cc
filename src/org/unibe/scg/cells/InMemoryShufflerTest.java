package org.unibe.scg.cells;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

@SuppressWarnings("javadoc")
public final class InMemoryShufflerTest {
	private InMemoryShuffler<Byte> s;
	private Cell<Byte> aaad0;
	private Cell<Byte> aaad1;

	@Before
	public void setUp() {
		s = new InMemoryShuffler<>();
		s.write(new Cell<Byte>(ByteString.copyFromUtf8("aaab"), ByteString.copyFromUtf8("0"), ByteString.EMPTY));
		s.write(new Cell<Byte>(ByteString.copyFromUtf8("aaac"), ByteString.EMPTY, ByteString.EMPTY));
		aaad0 = new Cell<>(ByteString.copyFromUtf8("aaad"), ByteString.copyFromUtf8("0"), ByteString.EMPTY);
		s.write(aaad0);
		aaad1 = new Cell<>(ByteString.copyFromUtf8("aaad"), ByteString.copyFromUtf8("1"), ByteString.EMPTY);
		s.write(aaad1);
		s.close();
	}

	@Test
	public void testReadRow() {
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

		assertThat(Iterables.size(s.readRow(ByteString.EMPTY)), is(4));
	}

	@Test
	public void testReadColumn() {
		assertThat(Iterables.getOnlyElement(s.readColumn(ByteString.copyFromUtf8("1"))).getRowKey().toStringUtf8(),
				is("aaad"));
		assertThat(Iterables.size(s.readColumn(ByteString.EMPTY)), is(4));

		Iterable<Cell<Byte>> col0 = s.readColumn(ByteString.copyFromUtf8("0"));
		assertThat(Iterables.get(col0, 0).getRowKey().toStringUtf8(), is("aaab"));
		assertThat(Iterables.get(col0, 1).getRowKey().toStringUtf8(), is("aaad"));
		assertThat(col0.toString(), Iterables.size(col0), is(2));

		assertTrue(Iterables.isEmpty(s.readColumn(ByteString.copyFromUtf8("2"))));
		assertTrue(Iterables.isEmpty(s.readColumn(ByteString.copyFromUtf8("/"))));
	}
}
