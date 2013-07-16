package ch.unibe.scg.cc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

class InMemoryShuffler<T> implements CellSink<T>, CellSource<T> {
	final private List<Cell<T>> store = new ArrayList<>();

	@Override
	public void close() {
		Collections.sort(store);
	}

	@Override
	public void write(Cell<T> cell) {
		store.add(cell);
	}

	@Override
	public Iterable<Iterable<Cell<T>>> partitions() {
		if (store.isEmpty()) {
			return Collections.emptyList();
		}

		ImmutableList.Builder<Iterable<Cell<T>>> ret = ImmutableList.builder();

		ImmutableList.Builder<Cell<T>> partition = ImmutableList.builder();
		Cell<T> last = null;

		for (Cell<T> c : store) {
			if ((last != null) && (!c.rowKey.equals(last.rowKey))) {
				ret.add(partition.build());
				partition = ImmutableList.builder();
				last = null;
			}

			if (!c.equals(last)) {
				partition.add(c);
			}

			last = c;
		}

		ret.add(partition.build());

		return ret.build();
	}
}
