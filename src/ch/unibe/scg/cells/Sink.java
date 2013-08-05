package ch.unibe.scg.cells;

import java.io.Closeable;
import java.io.IOException;

/** The output of a mapper is written into a sink. Closing closes the underlying CellSink. */
public interface Sink<T> extends Closeable {
	/** Write {@code object} into the sink. */
	void write(T object) throws IOException;
}
