package ch.unibe.scg.cc.mappers;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import javax.inject.Named;
import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultithreadedTableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import ch.unibe.scg.cc.CloneExpander;
import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.CloneGroup;
import ch.unibe.scg.cc.Protos.CloneGroup.Builder;
import ch.unibe.scg.cc.Protos.Occurrence;
import ch.unibe.scg.cc.SpamDetector;
import ch.unibe.scg.cc.WrappedRuntimeException;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;
import ch.unibe.scg.cc.mappers.CloneLoaderProvider.CloneLoader;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/** See paper. */
public class MakeFunction2FineClones implements Runnable {
	static final String OUT_DIR = "/tmp/fineclones/";
	static Logger logger = Logger.getLogger(MakeFunction2FineClones.class.getName());
	private final HTable function2fineclones;
	private final MapReduceLauncher launcher;
	private final Provider<Scan> scanProvider;

	@Inject
	MakeFunction2FineClones(MapReduceLauncher launcher, @Named("function2fineclones") HTable function2fineclones,
			Provider<Scan> scanProvider) {
		this.launcher = launcher;
		this.function2fineclones = function2fineclones;
		this.scanProvider = scanProvider;
	}

	static class Function2FineClones {
		final CloneExpander cloneExpander;
		final LoadingCache<byte[], String> cloneLoader;
		final SpamDetector spamDetector;
		final StringOfLinesFactory stringOfLinesFactory;

		// Optional because in MRMain, we have an injector that does not set
		// this property, and can't, because it doesn't have the counter
		// available.
		@Inject(optional = true)
		@Named(Constants.COUNTER_CLONES_REJECTED)
		Counter clonesRejected;

		@Inject(optional = true)
		@Named(Constants.COUNTER_CLONES_PASSED)
		Counter clonesPassed;

		@Inject
		Function2FineClones(CloneExpander cloneExpander, @CloneLoader LoadingCache<byte[], String> cloneLoader,
				SpamDetector spamDetector, StringOfLinesFactory stringOfLinesFactory) {
			this.cloneExpander = cloneExpander;
			this.cloneLoader = cloneLoader;
			this.spamDetector = spamDetector;
			this.stringOfLinesFactory = stringOfLinesFactory;
		}

		/**
		 * Filter clones down to the clones that aren't spam. In case an
		 * IOException occurs, abort. Otherwise, try on and log error.
		 */
		Iterable<Clone> transform(Iterable<Clone> matches) throws IOException {
			Collection<Clone> clones = cloneExpander.expandClones(matches);
			Collection<Clone> ret = new ArrayList<>();
			for (Clone clone : clones) {
				try {
					if (!spamDetector.isSpamByParameters(spamDetector.extractFeatureVector(
							stringOfLinesFactory.make(cloneLoader.get(clone.getThisSnippet().getFunction().toByteArray())).getLines(
									clone.getThisSnippet().getPosition(), clone.getThisSnippet().getLength()),
							stringOfLinesFactory.make(cloneLoader.get(clone.getThatSnippet().getFunction().toByteArray())).getLines(
									clone.getThatSnippet().getPosition(), clone.getThatSnippet().getLength())))) {
						ret.add(clone);
					}
				} catch (ExecutionException e) {
					Throwables.propagateIfPossible(e.getCause(), IOException.class);
					logger.severe("Failure while trying to load sources for " + clone + e.getCause());
				}
			}
			if (clonesRejected != null) {
				clonesRejected.increment(clones.size() - ret.size());
			}
			if (clonesPassed != null) {
				clonesPassed.increment(ret.size());
			}
			return ret;
		}
	}

	static class MakeFunction2FineClonesMapper extends GuiceTableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		final Function2FineClones function2FineClones;

		@Inject
		MakeFunction2FineClonesMapper(Function2FineClones function2FineClones) {
			this.function2FineClones = function2FineClones;
		}

		/** receives rows from htable function2roughclones */
		@Override
		public void map(ImmutableBytesWritable uselessKey, Result value, Context context) throws IOException,
				InterruptedException {
			context.getCounter(Counters.FUNCTIONS).increment(1);

			Collection<Clone> matches = new ArrayList<>();
			for (Entry<byte[], byte[]> col : value.getFamilyMap(Constants.FAMILY).entrySet()) {
				try {
					matches.add(Clone.parseFrom(col.getValue()));
				} catch (InvalidProtocolBufferException e) {
					throw new RuntimeException("Could not decode " + Arrays.toString(col.getValue()), e);
				}
			}

			// matching is symmetrical - so we do only half of it here
			// after matching procedure we expand to full clones
			Iterable<Clone> transformed = function2FineClones.transform(matches);
			for (Clone clone : transformed) {
				context.write(new ImmutableBytesWritable(clone.getThisSnippet().getFunction().toByteArray()),
						new ImmutableBytesWritable(clone.toByteArray()));
			}
		}
	}

	static class MakeFunction2FineClonesReducer extends
			GuiceReducer<ImmutableBytesWritable, ImmutableBytesWritable, BytesWritable, NullWritable> {
		final private LoadingCache<byte[], String> functionStringCache;

		final private StringOfLinesFactory stringOfLinesFactory;
		// Optional because in MRMain, we have an injector that does not set
		// this property, and can't, because it doesn't have the counter
		// available.
		@Inject(optional = true)
		@Named(Constants.COUNTER_MAKE_FUNCTION_2_FINE_CLONES_ARRAY_EXCEPTIONS)
		Counter arrayExceptions;

		final private LoadingCache<ByteBuffer, Iterable<Occurrence>> fileLoader;
		final private LoadingCache<ByteBuffer, Iterable<Occurrence>> versionLoader;
		final private LoadingCache<ByteBuffer, Iterable<Occurrence>> projectLoader;

		@Inject
		MakeFunction2FineClonesReducer(StringOfLinesFactory stringOfLinesFactory,
				@CloneLoader LoadingCache<byte[], String> functionStringCache,
				@Named("file2function") LoadingCache<ByteBuffer, Iterable<Occurrence>> fileLoader,
				@Named("version2file") LoadingCache<ByteBuffer, Iterable<Occurrence>> versionLoader,
				@Named("project2version") LoadingCache<ByteBuffer, Iterable<Occurrence>> projectLoader) {
			this.stringOfLinesFactory = stringOfLinesFactory;
			this.functionStringCache = functionStringCache;
			this.fileLoader = fileLoader;
			this.versionLoader = versionLoader;
			this.projectLoader = projectLoader;
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
			Builder cloneGroupBuilder = CloneGroup.newBuilder();

			cloneGroupBuilder.addAllOccurrences(findOccurrences(ByteBuffer.wrap(functionHashKey.get())));

			for (ImmutableBytesWritable cloneProtobuf : cloneProtobufs) {
				final Clone clone = Clone.parseFrom(cloneProtobuf.get());
				checkState(Arrays.equals(clone.getThisSnippet().getFunction().toByteArray(), functionHashKey.get()),
						"The function hash key did not match one of the clones. Clone says: "
								+ BaseEncoding.base16().encode(clone.getThisSnippet().getFunction().toByteArray())
								+ " reduce key: " + BaseEncoding.base16().encode(functionHashKey.get()));

				if (!clone.getThisSnippet().getFunction().equals(ByteString.copyFrom(functionHashKey.get()))) {
					throw new AssertionError(
							"There is a clone in cloneProtobufs that doesn't match the input function "
									+ BaseEncoding.base16().encode(functionHashKey.get()));
				}

				Collection<Occurrence> occ = findOccurrences(clone.getThatSnippet().getFunction().asReadOnlyByteBuffer());
				context.getCounter(Counters.OCCURRENCES).increment(occ.size());
				cloneGroupBuilder.addAllOccurrences(occ);

				from = Math.min(from, clone.getThisSnippet().getPosition());
				to = Math.max(to, clone.getThisSnippet().getPosition() + clone.getThisSnippet().getLength());
				commonness++;
			}

			if (commonness <= 0) {
				throw new AssertionError("commonness must be non-negative, but was " + commonness);
			}

			String functionString;
			try {
				functionString = functionStringCache.get(functionHashKey.get());
			} catch (ExecutionException e) {
				Throwables.propagateIfPossible(e.getCause(), IOException.class);
				throw new WrappedRuntimeException("The CacheLoader threw an exception while reading function "
						+ BaseEncoding.base16().encode(functionHashKey.get()) + ".", e.getCause());
			}

			cloneGroupBuilder.setText(
					stringOfLinesFactory.make(functionString, '\n').getLines(from, to - from));

			byte[] key = ColumnKeyCodec.encode(commonness, cloneGroupBuilder.build());
			context.write(new BytesWritable(key), NullWritable.get());
		}

		Cache<ByteBuffer, Collection<Occurrence>> occCache = CacheBuilder.newBuilder().maximumSize(10000)
				.concurrencyLevel(1).build();

		/** @return all occurrences of {@code functionKey} */
		private Collection<Occurrence> findOccurrences(final ByteBuffer functionKey) throws IOException {
			try {
				return occCache.get(functionKey, new Callable<Collection<Occurrence>>() {
					@Override public Collection<Occurrence> call() throws Exception {
						Collection<Occurrence> ret = new ArrayList<>();
						Iterable<Occurrence> files = fileLoader.get(functionKey);
						checkNotEmpty(files, Bytes.getBytes(functionKey), "files");

						for (Occurrence file : files) {
							Iterable<Occurrence> versions = versionLoader
									.get(file.getFileHash().asReadOnlyByteBuffer());
							checkNotEmpty(versions, file.getFileHash().toByteArray(), "versions");

							for (Occurrence version : versions) {
								Iterable<Occurrence> projects = projectLoader.get(version.getVersionHash()
										.asReadOnlyByteBuffer());
								checkNotEmpty(projects, version.getVersionHash().toByteArray(), "projects");

								for (Occurrence project : projects) {
									ret.add(Occurrence.newBuilder()
											.mergeFrom(file)
											.mergeFrom(version)
											.mergeFrom(project).build());
								}
							}
						}

						return ret;
					}

					private void checkNotEmpty(Iterable<Occurrence> i, byte[] hash, String table) {
						if (Iterables.isEmpty(i)) {
							logger.severe("Found no " + table + " for hash "
									+ BaseEncoding.base16().encode(hash).substring(0, 6));
						}
					}
				});
			} catch (ExecutionException e) {
				Throwables.propagateIfPossible(e.getCause(), IOException.class);
				throw new RuntimeException(e);
			}
		}
	}

	static class ColumnKeyCodec {
		static byte[] encode(int commonness, CloneGroup cloneGroup) {
			checkArgument(commonness >= 0, "Negative commonness will ruin sorting. You supplied commonness "
					+ commonness);
			return Bytes.add(Bytes.toBytes(commonness), cloneGroup.toByteArray());
		}

		static ColumnKey decode(byte[] encoded) throws InvalidProtocolBufferException {
			return new ColumnKey(Bytes.toInt(Bytes.head(encoded, Ints.BYTES)), CloneGroup.parseFrom(Bytes.tail(
					encoded, encoded.length - Ints.BYTES)));
		}
	}

	static class ColumnKey {
		final int commonness;
		final CloneGroup cloneGroup;

		ColumnKey(int commonness, CloneGroup cloneGroup) {
			this.commonness = commonness;
			this.cloneGroup = cloneGroup;
		}
	}

	@Override
	public void run() {
		try {
			FileSystem.get(new Configuration()).delete(new Path(OUT_DIR), true);
			launcher.truncate(function2fineclones);

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
			config.setInt(MRJobConfig.MAP_MEMORY_MB, 8192);
			config.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx6000M");
			config.setInt(MRJobConfig.REDUCE_MEMORY_MB, 1300);
			config.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx1000M");
			config.setInt(MultithreadedTableMapper.NUMBER_OF_THREADS, 20);
			config.set(FileOutputFormat.OUTDIR, OUT_DIR);
			config.setClass(Job.OUTPUT_FORMAT_CLASS_ATTR, SequenceFileOutputFormat.class, OutputFormat.class);
			config.setClass(Job.OUTPUT_KEY_CLASS, BytesWritable.class, Object.class);
			config.setClass(Job.OUTPUT_VALUE_CLASS, NullWritable.class, Object.class);
			config.set(Constants.GUICE_CUSTOM_MODULES_ANNOTATION_STRING, HBaseModule.class.getName());

			Scan scan = scanProvider.get();
			scan.addFamily(Constants.FAMILY);

			launcher.launchMapReduceJob(MakeFunction2FineClones.class.getName() + "Job", config,
					Optional.of("function2roughclones"), Optional.<String> absent(), Optional.of(scan),
					MakeFunction2FineClonesMapper.class.getName(),
					Optional.of(MakeFunction2FineClonesReducer.class.getName()), ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);
		} catch (IOException | ClassNotFoundException e) {
			throw new WrappedRuntimeException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return; // Exit.
		}
	}
}
