package org.unibe.scg.cells;

import java.io.IOException;

import ch.unibe.scg.cc.Sink;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

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
}
