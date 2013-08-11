package ch.unibe.scg.cells.hadoop;

import static com.google.common.io.BaseEncoding.base64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.lib.BinaryPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.CellSink;
import ch.unibe.scg.cells.Codec;
import ch.unibe.scg.cells.Codecs;
import ch.unibe.scg.cells.Mapper;
import ch.unibe.scg.cells.OfflineMapper;
import ch.unibe.scg.cells.OneShotIterable;
import ch.unibe.scg.cells.Pipeline;
import ch.unibe.scg.cells.Sink;
import ch.unibe.scg.cells.hadoop.TableAdmin.Table;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;

public class HadoopPipeline<IN, EFF> implements Pipeline<IN, EFF> {
	final private Table<IN> in;
	final private Table<EFF> efflux;
	final private ByteString family;
	final private TableAdmin admin;

	HadoopPipeline(Table<IN> in, Table<EFF> efflux, ByteString family, TableAdmin admin) {
		this.in = in;
		this.efflux = efflux;
		this.family = family;
		this.admin = admin;
	}

	@Override
	public MappablePipeline<IN, EFF> influx(Codec<IN> c) {
		return new HadoopMappablePipeline<>(in, c);
	}

	class HadoopMappablePipeline<I> implements MappablePipeline<I, EFF> {
		final private Codec<I> srcCodec;
		final private Table<I> src;

		HadoopMappablePipeline(Table<I> src, Codec<I> srcCodec) {
			this.src = src;
			this.srcCodec = srcCodec;
		}

		@Override
		public <E> ShuffleablePipeline<E, EFF> mapper(Provider<? extends Mapper<I, E>> m) {
			return new HadoopShuffleablePipelineAfterMap<>(src, srcCodec, m);
		}

		@Override
		public void efflux(Provider<? extends Mapper<I, EFF>> m, Codec<EFF> codec) throws IOException {
			throw new UnsupportedOperationException("I'll think about this case later :)");
		}

		@Override
		public void effluxWithOfflineMapper(Provider<? extends OfflineMapper<I, EFF>> offlineMapper, Codec<EFF> codec)
				throws IOException {
			throw new UnsupportedOperationException("I'll think about this case later :)");
		}
	}

	class HadoopReducablePipeline<MAP_IN, MAP_OUT> implements MappablePipeline<MAP_OUT, EFF> {
		final private Table<MAP_IN> src;
		final private Codec<MAP_IN> mapSrcCodec;
		final private Provider<? extends Mapper<MAP_IN, MAP_OUT>> map;

		final private Codec<MAP_OUT> reduceSrcCodec;

		HadoopReducablePipeline(Table<MAP_IN> src, Codec<MAP_IN> mapSrcCodec,
				Provider<? extends Mapper<MAP_IN, MAP_OUT>> map, Codec<MAP_OUT> reduceSrcCodec) {
			this.src = src;
			this.mapSrcCodec = mapSrcCodec;
			this.map = map;
			this.reduceSrcCodec = reduceSrcCodec;
		}

		@Override
		public <E> ShuffleablePipeline<E, EFF> mapper(Provider<? extends Mapper<MAP_OUT, E>> m) {
			return new HadoopShuffleablePipelineAfterReduce<>(src, mapSrcCodec, map, reduceSrcCodec, m);
		}

		@Override
		public void efflux(Provider<? extends Mapper<MAP_OUT, EFF>> m, Codec<EFF> codec) throws IOException, InterruptedException {
			run(src, mapSrcCodec, map, reduceSrcCodec, m, codec, efflux);
			src.close();
		}

		@Override
		public void effluxWithOfflineMapper(Provider<? extends OfflineMapper<MAP_OUT, EFF>> offlineMapper, Codec<EFF> codec)
				throws IOException {
			throw new RuntimeException("Not implemented");
		}
	}

	class HadoopShuffleablePipelineAfterMap<I, E> implements ShuffleablePipeline<E, EFF> {
		final private Table<I> src;
		final private Codec<I> srcCodec;
		final private Provider<? extends Mapper<I, E>> mapper;

		HadoopShuffleablePipelineAfterMap(Table<I> src, Codec<I> srcCodec,
				Provider<? extends Mapper<I, E>> mapper) {
			this.src = src;
			this.srcCodec = srcCodec;
			this.mapper = mapper;
		}

		@Override
		public MappablePipeline<E, EFF> shuffle(Codec<E> codec) throws IOException {
			return new HadoopReducablePipeline<>(src, srcCodec, mapper, codec);
		}
	}

	class HadoopShuffleablePipelineAfterReduce<MAP_IN, MAP_OUT, E> implements ShuffleablePipeline<E, EFF> {
		final private Table<MAP_IN> src;
		final private Codec<MAP_IN> mapSrcCodec;
		final private Provider<? extends Mapper<MAP_IN, MAP_OUT>> map;

		final private Codec<MAP_OUT> reduceSrcCodec;
		final private Provider<? extends Mapper<MAP_OUT, E>> reduce;

		HadoopShuffleablePipelineAfterReduce(Table<MAP_IN> src, Codec<MAP_IN> mapSrcCodec,
				Provider<? extends Mapper<MAP_IN, MAP_OUT>> map, Codec<MAP_OUT> reduceSrcCodec,
				Provider<? extends Mapper<MAP_OUT, E>> reduce) {
			this.src = src;
			this.mapSrcCodec = mapSrcCodec;
			this.map = map;
			this.reduceSrcCodec = reduceSrcCodec;
			this.reduce = reduce;
		}

		@Override
		public MappablePipeline<E, EFF> shuffle(Codec<E> codec) throws IOException, InterruptedException {
			Table<E> target = admin.createTemporaryTable();

			run(src, mapSrcCodec, map, reduceSrcCodec, reduce, codec, target);

			// This will delete temporary tables if needed.
			src.close();

			return new HadoopMappablePipeline<>(target, codec);
		}
	}

	<E, MAP_IN, MAP_OUT> void run(Table<MAP_IN> src, Codec<MAP_IN> mapSrcCodec,
			Provider<? extends Mapper<MAP_IN, MAP_OUT>> map, Codec<MAP_OUT> reduceSrcCodec,
			Provider<? extends Mapper<MAP_OUT, E>> reduce, Codec<E> codec, Table<E> target) throws IOException, InterruptedException {
		Job job = Job.getInstance();
		 // TODO: Set jar.
	     // TODO: Set input, output.

		 HadoopMapper<MAP_IN, MAP_OUT> hMapper = new HadoopMapper<>(map.get(), mapSrcCodec, reduceSrcCodec, family);
		 writeObjectToConf(job.getConfiguration(), hMapper);

		 HadoopReducer<MAP_OUT, E> hReducer = new HadoopReducer<>(reduce.get(), reduceSrcCodec, codec);
		 writeObjectToConf(job.getConfiguration(), hReducer);

		 Scan scan = HBaseCellSource.makeScan();
		 scan.addFamily(family.toByteArray());

		 // TODO: Set dependency jar.

	     TableMapReduceUtil.initTableMapperJob(
	    			src.getTableName(), // input table
	    			scan, // Scan instance to control CF and attribute selection
	    			DecoratorHadoopMapper.class, // mapper class
	    			ImmutableBytesWritable.class,         // mapper output key
	    			ImmutableBytesWritable.class,  // mapper output value
	    			job);
	    		TableMapReduceUtil.initTableReducerJob(
	    			new String(target.getTableName(), Charsets.UTF_8), // output table
	    			DecoratorHadoopReducer.class,    // reducer class
	    			job);

	     // Submit the job, then poll for progress until the job is complete
	     try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Loading of your job failed. ", e);
		}

	}

	static class DecoratorHadoopReducer<I, E> extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		private HadoopReducer<I, E> decorated;

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context)
				throws IOException, InterruptedException {
			decorated.reduce(key, values, context);
		}

		@SuppressWarnings("unchecked") // Unavoidable, since class literals cannot be generically typed.
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			decorated = readObjectFromConf(context.getConfiguration(), HadoopReducer.class);
			decorated.setup(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			decorated.cleanup(context);
		}
	}

	static class HadoopReducer<I, E> extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable>
			implements Serializable {
		static final private long serialVersionUID = 1L;

		final private Mapper<I, E> mapper;
		final private Codec<I> inCodec;
		final private Codec<E> outCodec;

		HadoopReducer(Mapper<I, E> mapper, Codec<I> inCodec, Codec<E> outCodec) {
			this.mapper = mapper;
			this.inCodec = inCodec;
			this.outCodec = outCodec;
		}

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context)
				throws IOException, InterruptedException {
			final ByteString rowKey = ByteString.copyFrom(key.get());

			Iterable<Cell<I>> row = Iterables.transform(values, new Function<ImmutableBytesWritable, Cell<I>>() {
				@Override public Cell<I> apply(ImmutableBytesWritable cellContent) {
					return Cell.<I> make(rowKey, ByteString.EMPTY, ByteString.copyFrom(cellContent.get()));
				}
			});

			// TODO: Make sink an instance var.
			runRow(Codecs.encode(makeSink(context), outCodec), Codecs.decodeRow(row, inCodec), mapper);

		}

		/** Format has to match {@link #readCell} below. */
		private CellSink<E> makeSink(final Context context) {
			return new CellSink<E>() {
				final private static long serialVersionUID = 1L;

				@Override
				public void close() throws IOException {
					// Nothing to close.
				}

				@Override
				public void write(Cell<E> cell) throws IOException, InterruptedException {
					byte[] rowKey = Bytes.concat(Ints.toByteArray(cell.getRowKey().size()),
							(cell.getRowKey().concat(cell.getColumnKey()).toByteArray()));

					// TODO: Shouldn't this be a Put?
					context.write(new ImmutableBytesWritable(rowKey),
							new ImmutableBytesWritable(cell.getCellContents().toByteArray()));
				}
			};
		}

		/** Redefined to escalate visibility. */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// Do nothing.
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			mapper.close();
		}
	}

	/** Loads the configured HadoopMapper and runs it. */
	static class DecoratorHadoopMapper<I, E> extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		private HadoopMapper<I, E> decorated;

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			decorated.map(key, value, context);
		}

		@SuppressWarnings("unchecked") // Unavoidable, since class literals cannot be generically typed.
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			decorated = readObjectFromConf(context.getConfiguration(), HadoopMapper.class);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			decorated.cleanup(context);
		}
	}

	/** Read obj from conf from key obj.getClassName(), in base64 encoding of its java serialization. */
	static <T> T readObjectFromConf(Configuration conf, Class<T> clazz) throws IOException {
		try {
			return (T) new ObjectInputStream(new ByteArrayInputStream(base64().decode(
					conf.getRaw(clazz.getName())))).readObject();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Couldn't load " + clazz
					+ ". You probably didn't ship the JAR properly to the server. See the README", e);
		}
	}

	/** Write obj into conf under key obj.getClassName(), in base64 encoding of its java serialization. */
	static <T> void writeObjectToConf(Configuration conf, T obj) throws IOException {
		// Serialize into byte array.
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		new ObjectOutputStream(bos).writeObject(obj);
		byte[] serialized = bos.toByteArray();

		conf.set(obj.getClass().getName(), base64().encode(serialized));
	}

	static class HadoopMapper<I, E> extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable>
			implements Serializable {
		private static final long serialVersionUID = 1L;

		private final Mapper<I, E> mapper;
		private final Codec<I> inCodec;
		private final Codec<E> outCodec;
		/** Do not modify. */
		private final byte[] family;

		HadoopMapper(Mapper<I, E> mapper, Codec<I> inCodec, Codec<E> outCodec, ByteString family) {
			this.mapper = mapper;
			this.inCodec = inCodec;
			this.outCodec = outCodec;
			this.family = family.toByteArray();
		}

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			try (CellSink<E> cellSink = makeSink(context)) {
				Iterable<Cell<I>> cellRow = toCellRow(ByteString.copyFrom(key.get()),
						value.getFamilyMap(family));
				runRow(Codecs.encode(cellSink, outCodec), Codecs.decodeRow(cellRow, inCodec), mapper);
			}
		}

		/** Re-implemented to escalate visibility. */
		@Override
		protected void setup(Context context) {
			// Nothing to do.
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			mapper.close();
		}

		private Iterable<Cell<I>> toCellRow(ByteString rowKey, NavigableMap<byte[], byte[]> familyMap) {
			List<Cell<I>> ret = new ArrayList<>();
			for (Entry<byte[], byte[]> kv : familyMap.entrySet()) {
				ret.add(Cell.<I>make(rowKey, ByteString.copyFrom(kv.getKey()), ByteString.copyFrom(kv.getValue())));
			}
			return ret;
		}

		/** Format has to match {@link #readCell} below. */
		private CellSink<E> makeSink(final Context context) {
			return new CellSink<E>() {
				final private static long serialVersionUID = 1L;

				@Override
				public void close() throws IOException {
					// Nothing to close.
				}

				@Override
				public void write(Cell<E> cell) throws IOException, InterruptedException {
					byte[] rowKey = Bytes.concat(Ints.toByteArray(cell.getRowKey().size()),
							(cell.getRowKey().concat(cell.getColumnKey()).toByteArray()));

					context.write(new ImmutableBytesWritable(rowKey),
							new ImmutableBytesWritable(cell.getCellContents().toByteArray()));
				}
			};
		}
	}

	static class KeyGroupingPartitioner extends Partitioner<ImmutableBytesWritable, ImmutableBytesWritable>  {
		final private BinaryPartitioner<ImmutableBytesWritable> defaultPartitioner = new BinaryPartitioner<>();

		@Override
		public int getPartition(ImmutableBytesWritable key, ImmutableBytesWritable value, int parts) {
			return defaultPartitioner
					.getPartition(new BytesWritable(readCell(key, ByteString.EMPTY).getRowKey().toByteArray()), value, parts);
		}
	}

	static class KeyGroupingComparator extends WritableComparator {
		KeyGroupingComparator() {
			super(ImmutableBytesWritable.class);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Cell<Void> l = readCell((ImmutableBytesWritable) a, ByteString.EMPTY);
			Cell<Void> r = readCell((ImmutableBytesWritable) b, ByteString.EMPTY);
			return l.getRowKey().asReadOnlyByteBuffer().compareTo(r.getRowKey().asReadOnlyByteBuffer());
		}
	}

	static class KeySortingComparator extends WritableComparator {
		KeySortingComparator() {
			super(ImmutableBytesWritable.class);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			Cell<Void> l = readCell((ImmutableBytesWritable) a, ByteString.EMPTY);
			Cell<Void> r = readCell((ImmutableBytesWritable) b, ByteString.EMPTY);
			return l.compareTo(r);
		}
	}

	static Cell<Void> readCell(ImmutableBytesWritable rawWritable, ByteString cellContents) {
		// TODO: write column key twice.

		ByteBuffer raw = ByteBuffer.wrap(rawWritable.get());
		// Read len
		byte[] rawLen = new byte[4];
		raw.get(rawLen);
		int len = Ints.fromByteArray(rawLen);

		// Read row key
		byte[] rowKey = new byte[len];
		raw.get(rowKey);

		// Read column key.
		byte[] colKey = new byte[raw.remaining()];
		raw.get(colKey);

		return Cell.make(ByteString.copyFrom(rowKey), ByteString.copyFrom(colKey), cellContents);
	}

	private static <I, E> void runRow(Sink<E> sink, Iterable<I> row, Mapper<I, E> mapper) throws IOException, InterruptedException {
		Iterator<I> iter = row.iterator();
		I first = iter.next();
		Iterable<I> gluedRow = Iterables.concat(Arrays.asList(first), new OneShotIterable<>(iter));
		mapper.map(first, gluedRow, sink);
	}
}
