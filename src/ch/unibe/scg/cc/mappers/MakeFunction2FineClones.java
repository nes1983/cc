package ch.unibe.scg.cc.mappers;

import static ch.unibe.scg.cc.activerecord.Function.FUNCTION_SNIPPET;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import javax.inject.Named;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import ch.unibe.scg.cc.ByteUtils;
import ch.unibe.scg.cc.CloneExpander;
import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.SnippetLocation;
import ch.unibe.scg.cc.Protos.SnippetMatch;
import ch.unibe.scg.cc.WrappedRuntimeException;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class MakeFunction2FineClones implements Runnable {
	public static final String OUT_DIR = "/tmp/fineclones/";
	static Logger logger = Logger.getLogger(MakeFunction2FineClones.class.getName());
	final HTable function2fineclones;
	final HTable popularSnippets;
	final MRWrapper mrWrapper;

	@Inject
	MakeFunction2FineClones(MRWrapper mrWrapper, @Named("popularSnippets") HTable popularSnippets,
			@Named("function2fineclones") HTable function2fineclones) throws IOException {
		this.mrWrapper = mrWrapper;
		this.function2fineclones = function2fineclones;
		this.popularSnippets = popularSnippets;
	}

	public static class MakeFunction2FineClonesMapper extends
			GuiceTableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		CloneExpander cloneExpander;

		@Inject
		public MakeFunction2FineClonesMapper(CloneExpander cloneExpander) {
			this.cloneExpander = cloneExpander;
		}

		/** receives rows from htable function2roughclones */
		@Override
		public void map(ImmutableBytesWritable uselessKey, Result value,
				@SuppressWarnings("rawtypes") org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException,
				InterruptedException {
			final byte[] function = value.getRow();
			assert function.length == 20;

			logger.finer("map function " + ByteUtils.bytesToHex(function));

			NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(GuiceResource.FAMILY);
			Set<Entry<byte[], byte[]>> columns = familyMap.entrySet();
			Iterable<SnippetMatch> matches = Iterables.transform(columns,
					new Function<Entry<byte[], byte[]>, SnippetMatch>() {
						public SnippetMatch apply(Entry<byte[], byte[]> cell) {
							// extract information from cellKey
							final ByteString thatFunction = ByteString.copyFrom(Bytes.head(cell.getKey(), 21));
							final int thisPosition = Bytes.toInt(Bytes.head(Bytes.tail(cell.getKey(), 8), 4));
							final int thisLength = Bytes.toInt(Bytes.tail(cell.getKey(), 4));

							// now reconstruct full SnippetLocations
							try {
								final SnippetMatch partialSnippetMatch = SnippetMatch.parseFrom(cell.getValue());
								return SnippetMatch
										.newBuilder(partialSnippetMatch)
										.setThisSnippetLocation(
												SnippetLocation
														.newBuilder(partialSnippetMatch.getThisSnippetLocation())
														.setFunction(ByteString.copyFrom(function))
														.setPosition(thisPosition).setLength(thisLength))
										.setThatSnippetLocation(
												SnippetLocation
														.newBuilder(partialSnippetMatch.getThatSnippetLocation())
														.setFunction(thatFunction)).build();
							} catch (InvalidProtocolBufferException e) {
								throw new WrappedRuntimeException(e);
							}
						}
					});

			// matching is symmetrical - so we do only half of it here
			Collection<Clone> clones = cloneExpander.expandClones(matches);

			// after matching procedure we expand to full clones
			for (Clone clone : clones) {
				context.write(new ImmutableBytesWritable(clone.getThisFunction().toByteArray()),
						new ImmutableBytesWritable(clone.toByteArray()));
			}
		}
	}

	public static class MakeFunction2FineClonesReducer extends
			GuiceReducer<ImmutableBytesWritable, ImmutableBytesWritable, CommonSnippetWritable, NullWritable> {
		final static Cache<byte[], String> functionStringCache = CacheBuilder.newBuilder().maximumSize(10000)
				.concurrencyLevel(1).build();
		final HTable strings;
		final StringOfLinesFactory stringOfLinesFactory;
		// Optional because in MRMain, we have an injector that does not set
		// this property, and can't, because it doesn't have the counter
		// available.
		@Inject(optional = true)
		@Named(GuiceResource.COUNTER_MAKE_FUNCTION_2_FINE_CLONES_ARRAY_EXCEPTIONS)
		Counter arrayExceptions;

		@Inject
		MakeFunction2FineClonesReducer(@Named("strings") HTable strings, StringOfLinesFactory stringOfLinesFactory) {
			this.strings = strings;
			this.stringOfLinesFactory = stringOfLinesFactory;
		}

		@Override
		public void reduce(final ImmutableBytesWritable functionHashKey,
				Iterable<ImmutableBytesWritable> cloneProtobufs, Context context) throws IOException,
				InterruptedException {
			// there's always the same iterator instance returned when calling
			// cloneProtobufs.iterator()

			int from = Integer.MAX_VALUE;
			int to = Integer.MIN_VALUE;

			int commonness = 0;
			for (ImmutableBytesWritable cloneProtobuf : cloneProtobufs) {
				final Clone clone = Clone.parseFrom(cloneProtobuf.get());
				if (!clone.getThisFunction().equals(ByteString.copyFrom(functionHashKey.get()))) {
					throw new AssertionError(
							"There is a clone in cloneProtobufs that doesn't match the input function "
									+ ByteUtils.bytesToHex(functionHashKey.get()));
				}
				from = Math.min(from, clone.getThisFromPosition());
				to = Math.max(to, clone.getThisFromPosition() + clone.getThisLength());
				commonness++;
			}

			if (commonness <= 0)
				new AssertionError("commonness must be non-negative, but was " + commonness);

			String functionString;
			try {
				functionString = functionStringCache.get(functionHashKey.get(), new Callable<String>() {
					@Override
					public String call() throws Exception {
						return Bytes.toString(strings.get(new Get(functionHashKey.get())).getValue(
								GuiceResource.FAMILY, FUNCTION_SNIPPET));
					}
				});
			} catch (ExecutionException e) {
				throw new WrappedRuntimeException("The CacheLoader threw an exception while reading function "
						+ ByteUtils.bytesToHex(functionHashKey.get()) + ".", e);
			}

			try {
				String sol = stringOfLinesFactory.make(functionString, '\n').getLines(from, to - from);
				context.write(new CommonSnippetWritable(commonness, sol), NullWritable.get());
			} catch (ArrayIndexOutOfBoundsException e) {
				logger.severe("ArrayIndexOutOfBoundsException: Tried to access from: " + from + " / to: " + to
						+ " on functionString: \n\n" + functionString);
				arrayExceptions.increment(1);
			}
		}
	}

	/**
	 * Used as the output format. The output is primarily be sorted by
	 * commonness, then alphabetically by snippet.
	 */
	static class CommonSnippetWritable implements WritableComparable<CommonSnippetWritable> {
		private int commonness;
		private String snippet;

		public CommonSnippetWritable() {
		}

		CommonSnippetWritable(int commonness, String snippet) {
			this.commonness = commonness;
			this.snippet = snippet;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			commonness = in.readInt();
			snippet = in.readUTF();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(commonness);
			out.writeUTF(snippet);
		}

		@Override
		public int compareTo(CommonSnippetWritable other) {
			return ComparisonChain.start().compare(commonness, other.commonness).compare(snippet, other.snippet)
					.result();
		}

		@Override
		public String toString() {
			return String.format("%d%n%s%n", commonness, snippet);
		}
	}

	@Override
	public void run() {
		try {
			FileSystem.get(new Configuration()).delete(new Path(OUT_DIR), true);
			mrWrapper.truncate(function2fineclones);

			Scan scan = new Scan();
			scan.setCaching(100); // TODO play with this. (100 is default value)
			scan.setCacheBlocks(false);
			scan.addFamily(GuiceResource.FAMILY); // Gets all columns from the
													// specified family.

			Configuration config = new Configuration();
			config.set(MRJobConfig.MAP_LOG_LEVEL, "DEBUG");
			config.set(MRJobConfig.NUM_REDUCES, "30");
			// TODO test that
			config.set(MRJobConfig.REDUCE_MERGE_INMEM_THRESHOLD, "0");
			config.set(MRJobConfig.REDUCE_MEMTOMEM_ENABLED, "true");
			config.set(MRJobConfig.IO_SORT_MB, "512");
			config.set(MRJobConfig.IO_SORT_FACTOR, "100");
			config.set(MRJobConfig.JOB_UBERTASK_ENABLE, "true");
			config.set(MRJobConfig.TASK_TIMEOUT, "86400000");
			config.set(MRJobConfig.MAP_MEMORY_MB, "8192");
			config.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx8000M");
			config.set(MRJobConfig.REDUCE_MEMORY_MB, "3072");
			config.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx2560M");
			config.set(FileOutputFormat.OUTDIR, OUT_DIR);
			config.setClass(Job.OUTPUT_FORMAT_CLASS_ATTR, SequenceFileOutputFormat.class, OutputFormat.class);
			config.setClass(Job.OUTPUT_KEY_CLASS, CommonSnippetWritable.class, Object.class);
			config.setClass(Job.OUTPUT_VALUE_CLASS, NullWritable.class, Object.class);

			mrWrapper.launchMapReduceJob(MakeFunction2FineClones.class.getName() + "Job", config,
					Optional.of("function2roughclones"), Optional.<String> absent(), Optional.of(scan),
					MakeFunction2FineClonesMapper.class.getName(),
					Optional.of(MakeFunction2FineClonesReducer.class.getName()), ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);
		} catch (IOException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (InterruptedException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (ClassNotFoundException e) {
			throw new WrappedRuntimeException(e.getCause());
		}
	}
}
