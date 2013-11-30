package ch.unibe.scg.cells;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

@SuppressWarnings("javadoc")
public final class InMemorySourceTest {
	private InMemorySource<Void> s;

	@Before
	public void setUp() {
		s = new InMemorySource<>(Arrays.asList(
			Arrays.asList(
				new Cell<Void>(ByteString.copyFromUtf8("aa0b"), ByteString.copyFromUtf8("1"), ByteString.EMPTY),
				new Cell<Void>(ByteString.copyFromUtf8("aaab"), ByteString.copyFromUtf8("1"), ByteString.EMPTY),
				new Cell<Void>(ByteString.copyFromUtf8("aaac"), ByteString.copyFromUtf8("1"), ByteString.EMPTY),
				new Cell<Void>(ByteString.copyFromUtf8("aaac"), ByteString.copyFromUtf8("2"), ByteString.EMPTY)),
			Arrays.asList(
				new Cell<Void>(ByteString.copyFromUtf8("aaad"), ByteString.copyFromUtf8("0"), ByteString.EMPTY)),
			Collections.<Cell<Void>> emptyList(),
			Arrays.asList(
				new Cell<Void>(ByteString.copyFromUtf8("aaae"), ByteString.copyFromUtf8("0"), ByteString.EMPTY),
				new Cell<Void>(ByteString.copyFromUtf8("aab"), ByteString.copyFromUtf8("0"), ByteString.EMPTY)),
			Arrays.asList(
				new Cell<Void>(ByteString.copyFrom(new byte[] {-1, 'a', 'd'}), ByteString.copyFromUtf8("0"), ByteString.EMPTY))));
	}

	@Test
	public void testIterator() {
		assertThat(Iterables.toString(s),
				is("[[[[97, 97, 48, 98]]{[49]}], "
					+ "[[[97, 97, 97, 98]]{[49]}], "
					+ "[[[97, 97, 97, 99]]{[49]}, [[97, 97, 97, 99]]{[50]}], "
					+ "[[[97, 97, 97, 100]]{[48]}], "
					+ "[[[97, 97, 97, 101]]{[48]}], "
					+ "[[[97, 97, 98]]{[48]}], "
					+ "[[[-1, 97, 100]]{[48]}]]"));
	}

	@Test
	public void testReadRowFF() {
		Iterable<Cell<Void>> row = s.readRow(ByteString.copyFrom(new byte[] {-1}));
		assertThat(Iterables.toString(row), is("[[[-1, 97, 100]]{[48]}]"));

		row = s.readRow(ByteString.copyFromUtf8("aaa"));
		assertThat(Iterables.toString(row),
				is("[[[97, 97, 97, 98]]{[49]}, [[97, 97, 97, 99]]{[49]}, "
					+ "[[97, 97, 97, 99]]{[50]}, [[97, 97, 97, 100]]{[48]}, [[97, 97, 97, 101]]{[48]}]"));
	}

	@Test
	public void testReadRowBeyondShards() {
		Iterable<Cell<Void>> row = s.readRow(ByteString.copyFrom(new byte[] {-1, -1}));
		assertThat(Iterables.isEmpty(row), is(true));

		row = s.readRow(ByteString.copyFrom(new byte[] {-1, -1, 0}));
		assertThat(Iterables.isEmpty(row), is(true));
	}

	@Test
	public void emptyStore() {
		InMemorySource<Void> source = new InMemorySource<>(Collections.<List<Cell<Void>>> emptyList());
		assertThat(Iterables.isEmpty(source), is(true));
		assertThat(Iterables.isEmpty(source.readRow(ByteString.copyFromUtf8("bla"))), is(true));

		source = new InMemorySource<>(Arrays.<List<Cell<Void>>> asList(
				Collections.<Cell<Void>> emptyList(), Collections.<Cell<Void>> emptyList()));
		assertThat(Iterables.isEmpty(source), is(true));
		assertThat(Iterables.isEmpty(source.readRow(ByteString.copyFromUtf8("bla"))), is(true));
	}

	@Test
	public void readFromSameShard() {
		Iterable<Cell<Void>> row = s.readRow(ByteString.copyFromUtf8("aaab"));
		assertThat(Iterables.toString(row), is("[[[97, 97, 97, 98]]{[49]}]"));

		row = s.readRow(ByteString.copyFromUtf8("aa0b"));
		assertThat(Iterables.toString(row), is("[[[97, 97, 48, 98]]{[49]}]"));
	}
}
