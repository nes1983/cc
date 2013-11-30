package ch.unibe.scg.cells;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

import com.google.protobuf.ByteString;

/** A lookup table is a generalized bigtable, allowing access by row and column. */
// TODO: Missing feature: exact row lookup, column lookup.
public interface CellLookupTable<T> extends Closeable, Serializable {
	/** @return an undecoded row of cells. */
	Iterable<Cell<T>> readRow(ByteString rowKeyPrefix) throws IOException;

	/** @return an undecoded column of cells. */
	Iterable<Cell<T>> readColumn(ByteString columnKeyPrefix) throws IOException;
}
