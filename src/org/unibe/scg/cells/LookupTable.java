package org.unibe.scg.cells;

import com.google.protobuf.ByteString;

/** A lookup table is a decoded {@link CellLookupTable}. */
public interface LookupTable<T> {
	/** Return a table row. */
	Iterable<T> readRow(ByteString rowKey);
}