package ch.unibe.scg.cells;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

import com.google.protobuf.ByteString;

/** A lookup table is a decoded {@link CellLookupTable}. Closing closes the underlying CellLookupTable. */
public interface LookupTable<T> extends Closeable, Serializable {
	/** Return a table row. */
	Iterable<T> readRow(ByteString rowKey) throws IOException;

	/** @return a decoded table column. */
	Iterable<T> readColumn(ByteString columnKey) throws IOException;
}