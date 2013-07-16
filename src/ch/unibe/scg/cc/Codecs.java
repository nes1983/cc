package ch.unibe.scg.cc;

import java.io.IOException;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

class Codecs {
	private Codecs() {} // Don't instantiate

	/** In case of a IOException, the iterator will throw an unchecked {@link EncodingException} */
	static <T> Iterable<T> decode(Iterable<Cell<T>> row, final Codec<T> codec) {
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

	static <T> Sink<T> encode(final CellSink<T> sink, final Codec<T> codec) {
		return new Sink<T>() {
			@Override public void write(T obj) {
				sink.write(codec.encode(obj));
			}
		};
	}
}
