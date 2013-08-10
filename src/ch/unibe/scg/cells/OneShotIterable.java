package ch.unibe.scg.cells;

import java.util.Iterator;

public class OneShotIterable<T> implements Iterable<T> {
	final private Iterator<T> underlying;
	private boolean iterated = false;

	public OneShotIterable(Iterator<T> underlying) {
		this.underlying = underlying;
	}

	@Override
	public Iterator<T> iterator() {
		if (iterated) {
			throw new IllegalStateException("Don't iterate a OneShotIterable twice.");
		}
		iterated = true;

		return underlying;
	}
}