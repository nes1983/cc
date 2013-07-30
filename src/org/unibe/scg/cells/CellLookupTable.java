package org.unibe.scg.cells;

import com.google.protobuf.ByteString;

/** A lookup table is a generalized bigtable, allowing access by row and column. */
// TODO: Missing features: lookup by prefix, lookup by row, mixes of the two.
public interface CellLookupTable<T> {
	/** Return an undecoded row of cells */
	Iterable<Cell<T>> readRow(ByteString rowKey);
}
