package ch.unibe.scg.cells;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.collect.UnmodifiableIterator;

/** Helper methods that help with Iterables of cells. */
enum Cells {
	; // Don't instantiate

	/** Implementation of {@link breakIntoRows}. */
	private static class Rows<T> extends UnmodifiableIterator<OneShotIterable<Cell<T>>> {
		/** Row to be returned on {@link #next}. */
		private List<Cell<T>> cur;
		/** The cell just behind `cur` in `iter`. */
		private Cell<T> next;
		final Iterator<Cell<T>> iter;

		Rows(Iterator<Cell<T>> iter) {
			this.iter = iter;
			if (iter.hasNext()) {
				next = iter.next();
			}
			forward();
		}

		@Override
		public boolean hasNext() {
			return cur != null;
		}

		@Override
		public OneShotIterable<Cell<T>> next() {
			if (!hasNext()) { // Demanded by Iterator contract.
				throw new NoSuchElementException();
			}
			Iterable<Cell<T>> ret = cur;
			forward();
			return new AdapterOneShotIterable<>(ret);
		}

		/** Assume next is set. Read in a row. Move `next` *past* row `cur`. */
		final void forward() { // final because it's called by constructor.
			cur = null;
			if (next == null) {
				return;
			}

			cur = new ArrayList<>();

			do {
				cur.add(next);
				// Pull in next. If you can't, set next to null.
				next = null;
				if (iter.hasNext()) {
					next = iter.next();
				}
				// If next fits into cur, loop.
			} while (next != null && next.getRowKey().equals(cur.get(0).getRowKey()));
		}
	}

	/** @return the iterable, broken into individiual rows. A row is defined in {@link Cell}. */
	static <T> Iterable<OneShotIterable<Cell<T>>> breakIntoRows(final Iterable<Cell<T>> shard) {
		return new Iterable<OneShotIterable<Cell<T>>>() {
			@Override public Iterator<OneShotIterable<Cell<T>>> iterator() {
				return new Rows<>(shard.iterator());
			}
		};
	}
}
