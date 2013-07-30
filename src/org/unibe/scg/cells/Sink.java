package org.unibe.scg.cells;

import java.io.Closeable;

/** The output of a mapper is written into a sink. Closing closes the underlying CellSink. */
// TODO: Should this be closeable?
public interface Sink<T> extends Closeable {
	/** Write {@code object} into the sink. */
	void write(T object);
}
