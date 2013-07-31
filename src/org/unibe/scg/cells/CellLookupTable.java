package org.unibe.scg.cells;

import java.io.Closeable;

import com.google.protobuf.ByteString;

/** A lookup table is a generalized bigtable, allowing access by row and column. */
// TODO: Missing features: lookup by prefix, mixes of row and column lookup.
public interface CellLookupTable<T> extends Closeable {
	/** @return an undecoded row of cells. */
	Iterable<Cell<T>> readRow(ByteString rowKey);

	/** @return an undecoded column of cells. */
	Iterable<Cell<T>> readColumn(ByteString columnKey);
}
