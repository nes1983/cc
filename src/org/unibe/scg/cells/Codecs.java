package org.unibe.scg.cells;

import java.io.IOException;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

/** Methods to help dealing with codecs */
public enum Codecs {
	; // Don't instantiate

	/** In case of a IOException, the iterator will throw an unchecked {@link EncodingException} */
	public static <T> Iterable<T> decode(Iterable<Cell<T>> row, final Codec<T> codec) {
		return Iterables.transform(row, new Function<Cell<T>, T>() {
			@Override public T apply(Cell<T> cell) {
				try {
					return codec.decode(cell);
				} catch (IOException e) {
					throw new EncodingException(e);
				}
			}
		});
	}

	/** Encode sink using codec */
	public static <T> Sink<T> encode(final CellSink<T> sink, final Codec<T> codec) {
		return new Sink<T>() {
			@Override public void write(T obj) {
				sink.write(codec.encode(obj));
			}
		};
	}

	/** Wrapper of {@code cellTable} that returns decoded rows */
	public static <T> LookupTable<T> decodedTable(final CellLookupTable<T> cellTable, final Codec<T> codec) {
		return new LookupTable<T>() {
			@Override public Iterable<T> readRow(ByteString rowKey) {
				return decode(cellTable.readRow(rowKey), codec);
			}
		};
	}
}
