package ch.unibe.scg.cells;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.protobuf.ByteString;

class InMemorySource<T> implements CellSource<T>, CellLookupTable<T> {
	private static final long serialVersionUID = 1L;

	/** Immutable. */
	final private List<List<Cell<T>>> store;
	final private Comparator<ByteString> cmp = new LexicographicalComparator();

	InMemorySource(List<List<Cell<T>>> store) {
		assert isStoreOk(store);
		this.store = store;
	}

	private static <T> boolean isStoreOk(List<List<Cell<T>>> store) {
		for (List<Cell<T>> shard : store) {
			if (!Ordering.<Cell<T>> natural().isOrdered(shard)) {
				return false;
			}
		}

		List<Cell<T>> prevShard = null;
		for (List<Cell<T>> cur : store) {
			if (cur.isEmpty()) {
				continue;
			}

			if (prevShard != null && prevShard.get(prevShard.size() - 1).compareTo(cur.get(0)) >= 0) {
				return false;
			}

			prevShard = cur;
		}

		Iterable<Cell<T>> flatStore = Iterables.concat(store);

		Cell<T> prevCell = null;
		for (Cell<T> c : flatStore) {
			if (c.equals(prevCell)) {
				return false;
			}
			prevCell = c;
		}

		return true;
	}

	@Override
	public Iterator<Iterable<Cell<T>>> iterator() {
		Iterable<Cell<T>> flatStore = Iterables.concat(store);

		if (Iterables.isEmpty(flatStore)) {
			return Collections.<Iterable<Cell<T>>> emptyList().iterator();
		}

		List<Iterable<Cell<T>>> ret = new ArrayList<>();
		List<Cell<T>> partition = new ArrayList<>();
		Cell<T> last = null;
		for (Cell<T> c : flatStore) {
			if ((last != null) && (!c.getRowKey().equals(last.getRowKey()))) {
				ret.add(partition);
				partition = new ArrayList<>();
				last = null;
			}

			partition.add(c);

			last = c;
		}

		ret.add(partition);

		return ret.iterator();
	}

	@Override
	public void close() {
		// Nothing to do.
	}

	@Override
	public Iterable<Cell<T>> readRow(ByteString rowKeyPrefix) {
		int fromShard = findShard(rowKeyPrefix);
		if (fromShard < 0) {
			return Collections.emptyList();
		}

		int from = Collections.binarySearch(
				store.get(fromShard),
				new Cell<T>(rowKeyPrefix, ByteString.EMPTY, ByteString.EMPTY));
		if (from < 0) {
			from = ~from; // ~from is the insertion point.
		}

		if (isKeyAllFF(rowKeyPrefix)) {
			assert fromShard == store.size() - 1;
			return store.get(fromShard).subList(from, store.get(fromShard).size());
		}

		ByteString toKey = ByteString.copyFrom(new BigInteger(rowKeyPrefix.toByteArray()).add(BigInteger.ONE)
				.toByteArray());
		int toShard = findShard(toKey);
		if (toShard < 0) {
			// Couldn't find a toShard. This can only happen if fromShard was in the last shard already.
			assert fromShard == store.size() - 1;
			toShard = fromShard;
		}

		int to = Collections.binarySearch(store.get(toShard), new Cell<T>(toKey, ByteString.EMPTY, ByteString.EMPTY));
		if (to >= 0) {
			to++; // To must be exclusive.
		} else {
			to = ~to; // To should be insertion point.
		}

		if (fromShard == toShard) {
			return store.get(fromShard).subList(from, to);
		}

		Iterable<Cell<T>> ret = store.get(fromShard).subList(from, store.get(fromShard).size());
		for (int i = fromShard + 1; i < toShard; i++) {
			ret = Iterables.concat(ret, store.get(i));
		}
		ret = Iterables.concat(ret, store.get(toShard).subList(0, to));

		return ret;
	}

	@Override
	public Iterable<Cell<T>> readColumn(ByteString columnKey) {
		throw new RuntimeException("Not implemented");
	}

	/** @return true if key consists only of bytes 0xff. False otherwise. */
	private boolean isKeyAllFF(ByteString key) {
		for (int i = 0; i < key.size(); i++) {
			if (key.byteAt(i) != -1) { // -1 signed == 0xff unsigned.
				return false;
			}
		}

		return true;
	}

	/** @return the index of shard that could contain the key prefix. -1 if there's none. */
	private int findShard(ByteString needle) {
		for (int i = 0; i < store.size(); i++) {
			List<Cell<T>> shard = store.get(i);
			if (!shard.isEmpty() && cmp.compare(needle, shard.get(shard.size() - 1).getRowKey()) <= 0) {
				return i;
			}
		}
		return -1;
	}
}
