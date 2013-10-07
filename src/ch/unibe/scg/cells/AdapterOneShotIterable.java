package ch.unibe.scg.cells;

import java.util.Iterator;

/** An adapter from Iterable to OneShotIterable. */
public final class AdapterOneShotIterable<T> implements OneShotIterable<T> {
	final private Iterator<T> underlying;
	private boolean iterated = false;

	/** underlying may not be null. */
	public AdapterOneShotIterable(Iterator<T> underlying) {
		this.underlying = underlying;
	}

	/** Iterable may not be null. */
	public AdapterOneShotIterable(Iterable<T> iterable) {
		this.underlying = iterable.iterator();
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
