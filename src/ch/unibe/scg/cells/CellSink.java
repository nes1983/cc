package ch.unibe.scg.cells;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;


/** All mappers write their outputs into {@link Sink}s, are ultimately owned by a CellSink */
public interface CellSink<T> extends Closeable, Serializable {
	/** Whenever a {@link Sink} issues a write, it is encoded and converted to a cell write. */
	void write(Cell<T> cell) throws IOException, InterruptedException;
}
