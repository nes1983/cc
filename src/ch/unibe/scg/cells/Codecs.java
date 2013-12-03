package ch.unibe.scg.cells;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

/** Methods to help dealing with codecs. */
public enum Codecs {
	; // Don't instantiate

	/** In case of a IOException, the iterator will throw an unchecked {@link EncodingException}. */
	public static <T> Iterable<T> decode(final Iterable<Cell<T>> row, final Codec<T> codec) {
		return Iterables.transform(row, new Function<Cell<T>, T>() {
			@Override public T apply(Cell<T> cell) {
				try {
					return codec.decode(cell);
				} catch (IOException e) {
					// TODO: Choose better exception.
					throw new EncodingException(e);
				}
			}
		});
	}

	/** @return the cell encoding of {@code objs}. */
	public static <T> Iterable<Cell<T>> encode(Iterable<T> objs, Codec<T> codec) {
		List<Cell<T>> ret = new ArrayList<>();
		for (T o : objs) {
			ret.add(codec.encode(o));
		}

		return ret;
	}

	/**
	 * Decode source using codec.
	 * In case of a IOException, the iterator will throw an unchecked {@link EncodingException}.
	 */
	// TODO: delete.
	public static <T> Source<T> decodeSource(final CellSource<T> source, final Codec<T> codec) {
		Iterable<Cell<T>> flatSource = Collections.emptyList();
		for (int i = 0; i < source.nShards(); i++) {
			flatSource = Iterables.concat(flatSource, source.getShard(i));
		}

		final Iterable<OneShotIterable<Cell<T>>> rows = Cells.breakIntoRows(flatSource);

		return new Source<T>() {
			final private static long serialVersionUID = 1L;

			@Override
			public Iterator<Iterable<T>> iterator() {
				return Iterables.transform(rows, new Function<Iterable<Cell<T>>, Iterable<T>>() {
					@Override public Iterable<T> apply(Iterable<Cell<T>> encodedRow) {
						return decode(encodedRow, codec);
					}
				}).iterator();
			}

			@Override public String toString() {
				return Iterators.toString(iterator());
			}

			@Override
			public void close() throws IOException {
				source.close();
			}
		};
	}

	/** Attempt to break {@code cells} into shards. Note that a custom sharding often works better. */
	public static <T> CellSource<T> shard(final Iterable<Cell<T>> cells) {
		final List<Cell<T>> cellsList = asList(cells);
		final int nShards = Math.min(2 * Runtime.getRuntime().availableProcessors(), cellsList.size());

		return new CellSource<T>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void close() throws IOException {
				// Nothing to do.
			}

			@Override
			public int nShards() {
				return nShards;
			}

			@Override
			public OneShotIterable<Cell<T>> getShard(int shard) {
				int shardSize = cellsList.size() / nShards;
				List<Cell<T>> ret;
				if (shard == nShards - 1) {
					// Last shard goes unto the end.
					ret = cellsList.subList(shard * shardSize, cellsList.size());
				} else {
					ret = cellsList.subList(shard * shardSize, (shard + 1) * shardSize);
				}

				return new AdapterOneShotIterable<>(ret);
			}
		};
	}

	/**
	 * Encode sink using codec.
	 *
	 * @return a sink that closes the wrapped sink on close.
	 */
	public static <T> Sink<T> encode(final CellSink<T> sink, final Codec<T> codec) {
		return new Sink<T>() {
			final static private long serialVersionUID = 1L;

			@Override public void write(T obj) throws IOException, InterruptedException {
				sink.write(codec.encode(obj));
			}

			@Override public void close() throws IOException {
				sink.close();
			}
		};
	}

	/** Wrapper of {@code cellTable} that returns decoded rows */
	public static <T> LookupTable<T> decodedTable(final CellLookupTable<T> cellTable, final Codec<T> codec) {
		return new LookupTable<T>() {
			final static private long serialVersionUID = 1L;

			@Override public Iterable<T> readRow(ByteString rowKey) throws IOException {
				return decode(cellTable.readRow(rowKey), codec);
			}

			@Override public void close() throws IOException {
				cellTable.close();
			}

			@Override public Iterable<T> readColumn(ByteString columnKey) throws IOException {
				return decode(cellTable.readColumn(columnKey), codec);
			}
		};
	}

	private static <T> List<Cell<T>> asList(final Iterable<Cell<T>> cells) {
		if (cells instanceof List) {
			return (List<Cell<T>>) cells;
		}

		return Lists.newArrayList(cells);
	}
}
