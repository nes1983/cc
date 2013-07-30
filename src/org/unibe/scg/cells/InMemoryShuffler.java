package org.unibe.scg.cells;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.protobuf.ByteString;

/** For running in memory, acts as both as sink and source */
public class InMemoryShuffler<T> implements CellSink<T>, CellSource<T>, CellLookupTable<T> {
	/** The backing store. When used as a sink, this is mutable. Closing the sink makes the field immutable. */
	private List<Cell<T>> store = new ArrayList<>();

	InMemoryShuffler() {} // Don't subclass

	/** Return an instance of a shuffler */
	public static <T> InMemoryShuffler<T> getInstance() {
		return new InMemoryShuffler<>();
	}

	@Override
	public void close() {
		// Sort store, discard duplicates, and make immutable.
		store = ImmutableList.copyOf(new TreeSet<>(store));
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
		int endPos = rowStartPos(ByteString.copyFrom(new BigInteger(rowKey.toByteArray()).add(BigInteger.ONE)
				.toByteArray()));

		return store.subList(startPos, endPos);
	}

	/**
	 * Sadly, built-in binary search does not guarantee to return the first
	 * possible match, so we've to walk backward after a binary search.
	 *
	 * @return startPos. If there is no such row, The insertion point is
	 *         returned. The returned value is nonnegative.
	 */
	private int rowStartPos(ByteString rowKey) {
		int pos = Collections.binarySearch(store, new Cell<T>(rowKey, ByteString.EMPTY, ByteString.EMPTY));
		if (pos < 0) { // Retrieve insertion point.
			pos = -pos - 1;
		}

		while (pos != 0 && store.get(pos-1).getRowKey().equals(rowKey)) {
			pos--;
		}

		assert pos >= 0;
		return pos;
	}
}
