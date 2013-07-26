package ch.unibe.scg.cc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

import com.google.common.collect.ComparisonChain;
import com.google.protobuf.ByteString;

/** Cell of an HTable. */
public final class Cell<T> implements Comparable<Cell<T>>{
	final private ByteString rowKey;
	final private ByteString columnKey;
	final private ByteString cellContents;

	Cell(ByteString rowKey, ByteString columnKey, ByteString cellContents) {
		this.rowKey = checkNotNull(rowKey);
		this.columnKey = checkNotNull(columnKey);
		this.cellContents = checkNotNull(cellContents);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Objects.hashCode(rowKey);
		result = prime * result + Objects.hashCode(columnKey);

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
		final Comparator<ByteString> cmp = new Comparator<ByteString>() {
			@Override public int compare(ByteString o1, ByteString o2) {
				return o1.asReadOnlyByteBuffer().compareTo(o2.asReadOnlyByteBuffer());
			}
		};
		return ComparisonChain.start()
				.compare(rowKey, o.rowKey, cmp)
				.compare(columnKey, o.columnKey, cmp)
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
