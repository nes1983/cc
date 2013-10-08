package ch.unibe.scg.cells.hadoop;

import java.io.Closeable;

import ch.unibe.scg.cells.CellSource;

import com.google.protobuf.ByteString;

/**
 * Name of an HTable.
 *
 * @param <T> The type of the cells stored in the table. Must match the codec used to read it.
 */
public interface Table<T> extends Closeable {
	/** Encoding is assumed to be utf8. */
	String getTableName();

	/**
	 * The column family of the table. If an HBase table has more than one family, they're
	 * modeled as two cells tables.
	 */
	ByteString getFamilyName();

	/** @return a CellSource that reads all rows from this table. */
	CellSource<T> asCellSource();
}