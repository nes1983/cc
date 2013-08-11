package ch.unibe.scg.cells;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

/** The output of a mapper is written into a sink. Closing closes the underlying CellSink. */
public interface Sink<T> extends Closeable, Serializable {
	/** Write {@code object} into the sink. */
	void write(T object) throws IOException, InterruptedException;
}
