package ch.unibe.scg.cells.hadoop;

import java.io.Closeable;

/**
 * Name of an HTable.
 *
 * @param <T> The type of the cells stored in the table. Must match the codec used to read it.
 */
public interface Table<T> extends Closeable {
	/** Encoding is assumed to be utf8. */
	String getTableName();
}