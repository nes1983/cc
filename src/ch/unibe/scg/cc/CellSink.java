package ch.unibe.scg.cc;

import java.io.Closeable;

interface CellSink<T> extends Closeable {
	void write(Cell<T> cell);
}
