package org.unibe.scg.cells;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.protobuf.ByteString;

/** For running in memory, acts as both as sink and source */
public class InMemoryShuffler<T> implements CellSink<T>, CellSource<T>, CellLookupTable<T> {
	/** The backing store. When used as a sink, this is mutable. Closing the sink makes the field immutable. */
	private List<Cell<T>> store = new ArrayList<>();
	/** Becomes available upon close */
	private List<RowPointer> colIndex;

	InMemoryShuffler() {} // Don't subclass

	/** To look up a row, get a RowPointer, then look up its row in the store */
	private static class RowPointer implements Comparable<RowPointer> {
		final ByteString colKey;
		final ByteString rowKey;

		RowPointer(ByteString colKey, ByteString rowKey) {
			this.colKey = colKey;
			this.rowKey = rowKey;
		}

		@Override
		public int compareTo(RowPointer o) {
			return ComparisonChain
				.start()
				.compare(colKey.asReadOnlyByteBuffer(), o.colKey.asReadOnlyByteBuffer())
				.compare(rowKey.asReadOnlyByteBuffer(), o.rowKey.asReadOnlyByteBuffer())
				.result();
		}
	}

	/** Return an instance of a shuffler */
	public static <T> InMemoryShuffler<T> getInstance() {
		return new InMemoryShuffler<>();
	}

	@Override
	public void close() {
		// Sort store, discard duplicates, and make immutable.
		store = ImmutableList.copyOf(new TreeSet<>(store));

		List<RowPointer> colIndexBuilder = new ArrayList<>();
		// Produce colIndex
		for (Cell<T> c : store) {
			colIndexBuilder.add(new RowPointer(c.getColumnKey(), c.getRowKey()));
		}
		colIndex = Ordering.natural().immutableSortedCopy(colIndexBuilder);
	}

	@Override
	public void write(Cell<T> cell) {
		store.add(cell);
	}

	@Override
	public Iterator<Iterable<Cell<T>>> iterator() {
		if (store.isEmpty()) {
			return Iterators.emptyIterator();
		}

		ImmutableList.Builder<Iterable<Cell<T>>> ret = ImmutableList.builder();

		ImmutableList.Builder<Cell<T>> partition = ImmutableList.builder();
		Cell<T> last = null;

		for (Cell<T> c : store) {
			if ((last != null) && (!c.getRowKey().equals(last.getRowKey()))) {
				ret.add(partition.build());
				partition = ImmutableList.builder();
				last = null;
			}

			partition.add(c);

			last = c;
		}

		ret.add(partition.build());

		return ret.build().iterator();
	}

	@Override
	public Iterable<Cell<T>> readRow(ByteString rowKey) {
		assert Ordering.natural().isOrdered(store) : "Someone forgot to close the sink.";

		int startPos = rowStartPos(rowKey);
		// end position is binary search for rowKey + 1
		int endPos = rowStartPos(keyPlusOne(rowKey));

		return store.subList(startPos, endPos);
	}

	private ByteString keyPlusOne(ByteString rowKey) {
		return ByteString.copyFrom(new BigInteger(rowKey.toByteArray()).add(BigInteger.ONE)
				.toByteArray());
	}

	/**
	 * @return startPos of the row. If there is no such row, The insertion point is
	 *         returned. The returned value is nonnegative.
	 */
	private int rowStartPos(ByteString rowKey) {
		return retrieveInsertionPoint(Collections.binarySearch(store, new Cell<T>(rowKey, ByteString.EMPTY,
				ByteString.EMPTY)));
	}

	@Override
	public Iterable<Cell<T>> readColumn(ByteString columnKey) {
		int startPos = indexStartPos(columnKey);
		int endPos = indexStartPos(keyPlusOne(columnKey));
		List<RowPointer> rows = colIndex.subList(startPos, endPos);

		List<Cell<T>> ret = new ArrayList<>();
		for (RowPointer r : rows) {
			int p = Collections.binarySearch(store, new Cell<T>(r.rowKey, columnKey, ByteString.EMPTY));
			assert p >= 0 : "Index contained incorrect information for column " + columnKey;
			ret.add(store.get(p));
		}

		return ret;
	}

	private int indexStartPos(ByteString colKey) {
		return retrieveInsertionPoint(Collections.binarySearch(colIndex, new RowPointer(colKey, ByteString.EMPTY)));
	}

	private int retrieveInsertionPoint(int pos) {
		if (pos < 0) { // Retrieve insertion point.
			pos = -pos - 1;
		}

		assert pos >= 0;
		return pos;
	}
}
