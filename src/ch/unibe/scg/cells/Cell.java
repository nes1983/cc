package ch.unibe.scg.cells;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Comparator;

import com.google.common.collect.ComparisonChain;
import com.google.protobuf.ByteString;

/**
 * An encoded cell of data, modeled after bigtable cells.
 * A cell encodes an object of type {@code T}.
 * By convention, the encoding defines a grouping, and an ordering of cells.
 * Cells of the same row key are grouped into one row.
 * Inside of a row, cells are ordered by column key, in lexicographic order.
 *
 * @see Codec
 */
public final class Cell<T> implements Comparable<Cell<T>>{
	final private static Comparator<ByteString> byteStringCmp = new LexicographicalComparator();

	final private ByteString rowKey;
	final private ByteString columnKey;
	final private ByteString cellContents;

	Cell(ByteString rowKey, ByteString columnKey, ByteString cellContents) {
		this.rowKey = rowKey;
		this.columnKey = columnKey;
		this.cellContents = cellContents;
	}

	/** None of the parameters may be null. row and column must be non-empty. cell be empty. */
	public static <T> Cell<T> make(ByteString row, ByteString column, ByteString cell) {
		checkArgument(!checkNotNull(row).isEmpty(), "Should not be emptyish: " + row);
		checkArgument(!checkNotNull(column).isEmpty(), "Should not be emptyish: " + column);
		checkNotNull(cell);

		return new Cell<>(row, column, cell);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + rowKey.hashCode();
		result = prime * result + columnKey.hashCode();

		return result;
	}

	/** Two cells are equal if they agree on row key and column key. cellContents are not inspected! */
	@SuppressWarnings("unchecked") // Unavoidable. We cannot test if obj really is instanceof generic.
	@Override
	public boolean equals(Object obj) {
		return (obj == this)
				|| ((obj instanceof Cell) && compareTo((Cell<T>) obj) == 0);
	}

	/** Cells are sorted first by row key, then column key. cellContents are not inspected! */
	@Override
	public int compareTo(Cell<T> o) {
		return ComparisonChain.start()
				.compare(rowKey, o.rowKey, byteStringCmp)
				.compare(columnKey, o.columnKey, byteStringCmp)
				.result();
	}

	@Override
	public String toString() {
		return "[" + Arrays.toString(rowKey.toByteArray()) + "]{" + Arrays.toString(columnKey.toByteArray()) + "}";
	}

	/** Forms the key of the cell together with column key. */
	public ByteString getRowKey() {
		return rowKey;
	}

	/** Forms the key of the cell together with row key. */
	public ByteString getColumnKey() {
		return columnKey;
	}

	/** Payload of the cell */
	public ByteString getCellContents() {
		return cellContents;
	}
}
