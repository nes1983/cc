package ch.unibe.scg.cc;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

import com.google.common.collect.ComparisonChain;
import com.google.protobuf.ByteString;

/** Cell of an HTable. Two cells are equal if they agree on row key and column key. */
public final class Cell<T> implements Comparable<Cell<T>>{
	final ByteString rowKey;
	final ByteString columnKey;
	final ByteString cellContents;

	Cell(ByteString rowKey, ByteString columnKey, ByteString cellContents) {
		this.rowKey = rowKey;
		this.columnKey = columnKey;
		this.cellContents = cellContents;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Objects.hashCode(rowKey);
		result = prime * result + Objects.hashCode(columnKey);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Cell)) {
			return false;
		}
		@SuppressWarnings("unchecked") // Since we never really use type T, this will never fail.
		Cell<T> other = (Cell<T>) obj;
		return compareTo(other) == 0;
	}

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
}
