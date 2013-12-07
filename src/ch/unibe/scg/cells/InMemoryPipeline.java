package ch.unibe.scg.cells;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singletonList;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Provider;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.io.Closer;
import com.google.protobuf.ByteString;

/** Implementation of a {@link Pipeline} meant to run in memory. */
public class InMemoryPipeline<IN, OUT> implements Pipeline<IN, OUT>, Closeable {
	/** In seconds. */
	final private static int PRINT_INTERVAL = 1;
	/** In seconds. */
	final private static int SHUTDOWN_TIMEOUT = 20;
	/** How much to sample from each shard to get splitters. */
	final private static int SAMPLE_SIZE = 8;
	final private CellSource<IN> pipeSrc;
	/** Result of the last pipeline run. */
	private CellSource<OUT> pipeSink;
	final private PipelineStageScope scope;
	/** Synchronized set. */
	final private Provider<Set<LocalCounter>> registry;
	final private PrintStream out;
	final private ExecutorService threadPool = Executors.newFixedThreadPool(
			Runtime.getRuntime().availableProcessors());

	InMemoryPipeline(CellSource<IN> pipeSrc, PipelineStageScope scope,
			Provider<Set<LocalCounter>> counterRegistry, PrintStream out) { // Don't subclass.
		this.pipeSrc = pipeSrc;
		this.scope = scope;
		this.registry = counterRegistry;
		this.out = out;
	}

	/** Incredibly hacky way of moving an exception from one thread to another. */
	private static class ExceptionHolder {
		volatile Exception e; // volatile because it can be written and read from different threads.
	}

	/** A builder for a {@link InMemoryPipeline}. */
	public static class Builder {
		final private PipelineStageScope scope;
		final private Provider<Set<LocalCounter>> registry;

		@Inject
		Builder(PipelineStageScope scope, @CounterRegistry Provider<Set<LocalCounter>> registry) {
			this.scope = scope;
			this.registry = registry;
		}

		/** Create a pipeline that uses stderr for diagnostic print. No parameters are allowed to be null. */
		public <IN, OUT> InMemoryPipeline<IN, OUT> make(CellSource<IN> pipeSrc) {
			checkNotNull(pipeSrc);
			// counter info is diagnostic information, not actual output.
			return new InMemoryPipeline<>(pipeSrc, scope, registry, System.err);
		}
	}

	/** @return result of the last pipeline run. */
	public CellSource<OUT> lastEfflux() {
		return pipeSink;
	}

	@Override
	public MappablePipeline<IN, OUT> influx(Codec<IN> c) {
		return new InMemoryMappablePipeline<>(pipeSrc, c);
	}

	private class InMemoryMappablePipeline<I> implements MappablePipeline<I, OUT> {
		final private CellSource<I> src;
		final private Codec<I> srcCodec;

		InMemoryMappablePipeline(CellSource<I> src, Codec<I> srcCodec) {
			this.src = src;
			this.srcCodec = srcCodec;
		}

		@Override
		public <E> ShuffleablePipeline<E, OUT> map(Mapper<I, E> mapper) {
			return new InMemoryShuffleablePipeline<>(src, srcCodec, mapper);
		}

		@Override
		public void mapAndEfflux(Mapper<I, OUT> m, Codec<OUT> sinkCodec)
				throws IOException, InterruptedException {
			pipeSink = run(src, srcCodec, m, sinkCodec);
		}
	}

	private class InMemoryShuffleablePipeline<I, E> implements ShuffleablePipeline<E, OUT> {
		final private CellSource<I> src;
		final private Codec<I> srcCodec;
		final private Mapper<I, E> mapper;

		InMemoryShuffleablePipeline(CellSource<I> src, Codec<I> srcCodec, Mapper<I, E> mapper) {
			this.src = src;
			this.srcCodec = srcCodec;
			this.mapper = mapper;
		}

		@Override
		public MappablePipeline<E, OUT> shuffle(Codec<E> sinkCodec) throws IOException, InterruptedException {
			return new InMemoryMappablePipeline<>(run(src, srcCodec, mapper, sinkCodec), sinkCodec);
		}
	}

	/**
	 * Run the mapper. Closes source and mapper.
	 * If an exception occurs in any mapper, it is printed to {@code out} from within that thread.
	 * Any one of the exceptions is picked and returned to the caller of this method.
	 * The threadpool gets shut down on exceptions, too.
	 */
	// TODO: Perhaps report all exceptions as suppressed?
	private <I, E> CellSource<E> run(final CellSource<I> src, final Codec<I> srcCodec,
			final Mapper<I, E> mapper, final Codec<E> sinkCodec)
			throws IOException, InterruptedException {
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

		// Fork printing thread.
		Runnable printer = new Runnable() {
			@Override public void run() {
				printCounters();
			}
		};
		scheduler.scheduleAtFixedRate(printer, PRINT_INTERVAL, PRINT_INTERVAL, TimeUnit.SECONDS);

		// Run mappers and close source.
		final List<List<Cell<E>>> sinks = new ArrayList<>(src.nShards());
		for (int i = 0; i < src.nShards(); i++) {
			sinks.add(new ArrayList<Cell<E>>());
		}
		// If an exception occurs, write it in here.
		final ExceptionHolder exceptionHolder = new ExceptionHolder();
		try (final Closer closer = Closer.create()) {
			closer.register(mapper);
			closer.register(src);

			// Clone as many mappers as we have threads.
			final BlockingQueue<Mapper<I, E>> mappers = new ArrayBlockingQueue<>(
					Runtime.getRuntime().availableProcessors());
			for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
				@SuppressWarnings("resource") // It's getting registered for closing right here.
				Mapper<I, E> cloned = ShallowSerializingCopy.clone(mapper);
				closer.register(cloned);
				mappers.add(cloned);
			}

			// Run all rows in current shard through mapper.
			final CountDownLatch mapCnt = new CountDownLatch(src.nShards());
			for (int s = 0; s < src.nShards(); s++) {
				final int shard = s;
				threadPool.execute(new Runnable() {
					@Override public void run() {
						try {
							for (Iterable<Cell<I>> rawRow : Cells.breakIntoRows(src.getShard(shard))) {
								Iterable<I> decoded = Cells.decode(rawRow, srcCodec);
								Iterator<I> iter = decoded.iterator();
								final I first = iter.next();
								final Iterable<I> row = Iterables.concat(
										singletonList(first),
										new AdapterOneShotIterable<>(iter));
								@SuppressWarnings("resource") // Closed by mapperCloser.
								Mapper<I, E> m = mappers.take();
								m.map(
										first,
										new AdapterOneShotIterable<>(row),
										encode(sinks.get(shard), sinkCodec));
								mappers.put(m);

							}
						} catch (Exception e) {
							exceptionHolder.e = e;
							out.println("Suppressed exception:");
							e.printStackTrace(out);
							mapCnt.countDown();
							return;
						}
						mapCnt.countDown();
					}
				});
			}
			mapCnt.await();

			if (exceptionHolder.e != null) {
				Exception cause = exceptionHolder.e; // Don't let anybody overwrite the exception while handling.
				Throwables.propagateIfPossible(cause, IOException.class, InterruptedException.class);
				throw new RuntimeException(cause);
			}
		} finally {
			// to ensure counters will be printed upon completion.
			printCounters();
			if (scope != null) {
				scope.exit();
			}
			scheduler.shutdownNow();
			scheduler.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
		}

		// Filter out empty shards.
		final List<List<Cell<E>>> filteredSinks = new ArrayList<>();
		for (int shard = 0; shard < sinks.size(); shard++) {
			if (!sinks.get(shard).isEmpty()) {
				filteredSinks.add(sinks.get(shard));
			}
		}

		// Sort output shards
		final CountDownLatch sortCnt = new CountDownLatch(filteredSinks.size());
		for (int s = 0; s < filteredSinks.size(); s++) {
			final int shard = s;
			threadPool.execute(new Runnable() {
				@Override public void run() {
					Collections.sort(filteredSinks.get(shard));
					sortCnt.countDown();
				}
			});
		}
		sortCnt.await();

		// Suck filteredSinks into next source
		final CountDownLatch suckCnt = new CountDownLatch(filteredSinks.size());
		final List<ByteString> splitters = splitters(filteredSinks);
		final List<List<Cell<E>>> ret = new ArrayList<>(filteredSinks.size());
		for (int s = 0; s < filteredSinks.size(); s++) {
			ret.add(null); // Multithreaded `add` is forbidden, so grow beforehand.
		}
		for (int s = 0; s < filteredSinks.size() - 1; s++) { // All shards but last.
			final int shard = s;
			threadPool.execute(new Runnable() {
				@Override public void run() {
					List<Cell<E>> merged
						= merge(filteredSinks, splitters.get(shard), splitters.get(shard + 1));
					ret.set(shard, merged);
					suckCnt.countDown();
				}
			});
		}
		if (filteredSinks.size() > 0) { // Add last shard.
			threadPool.execute(new Runnable() {
				@Override public void run() {
					int lastShard = filteredSinks.size() - 1;
					List<Cell<E>> merged = mergeUnbounded(filteredSinks, splitters.get(lastShard));
					ret.set(lastShard, merged);
					suckCnt.countDown();
				}
			});
		}
		suckCnt.await();

		return InMemorySource.make(ret);
	}

	private static <T> Sink<T> encode(final Collection<Cell<T>> cellSink, final Codec<T> codec) {
		return new Sink<T>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void write(T obj) throws IOException, InterruptedException {
				cellSink.add(codec.encode(obj));
			}

			@Override
			public void close() throws IOException {
				// Nothing to do.
			}
		};
	}

	/**
	 * @return the sorted list of all entries in {@code sources} between {@code from} and {@code to}.
	 * @param sources collection of individually sorted lists.
	 * @param from first row key to be included, inclusive.
	 * @param to last row key to be included, exclusive.
	 */
	private static <T> List<Cell<T>> merge(Iterable<List<Cell<T>>> sources, ByteString from, ByteString to) {
		assert shardsOrdered(sources);

		Cell<T> fromProbe = new Cell<>(from, ByteString.EMPTY, ByteString.EMPTY);
		Cell<T> toProbe = new Cell<>(to, ByteString.EMPTY, ByteString.EMPTY);

		Set<Cell<T>> ret = new HashSet<>();
		for (List<Cell<T>> src : sources) {
			int fromPos = insertionPoint(fromProbe, src);
			int toPos = insertionPoint(toProbe, src);

			ret.addAll(src.subList(fromPos, toPos));
		}

		return Ordering.natural().immutableSortedCopy(ret);
	}

	/** Same as {@link #merge}, but assuming parameter {@code to} as infinite. */
	private static <T> List<Cell<T>> mergeUnbounded(Iterable<List<Cell<T>>> sources, ByteString from) {
		assert shardsOrdered(sources);

		Cell<T> fromProbe = new Cell<>(from, ByteString.EMPTY, ByteString.EMPTY);

		Set<Cell<T>> ret = new HashSet<>();
		for (List<Cell<T>> src : sources) {
			int fromPos = insertionPoint(fromProbe, src);
			ret.addAll(src.subList(fromPos, src.size()));
		}

		return Ordering.natural().immutableSortedCopy(ret);
	}

	/** @return the position {@code needle} in {@code needle}, or the insertion point, if absent. */
	private static <T> int insertionPoint(Cell<T> needle, List<Cell<T>> haystack) {
		int pos = Collections.binarySearch(haystack, needle);
		if (pos < 0) {
			pos = ~pos;
		}
		return pos;
	}

	/**
	 * @return row keys of the start of each partiton, inclusive, such that all partitions are equal size.
	 * 		The returned list has the same size as {@code sources}.
	 * @param sources each source should be sorted.
	 */
	private static <T> List<ByteString> splitters(Collection<List<Cell<T>>> sources) {
		assert shardsOrdered(sources);
		assert !sources.isEmpty();

		// Grab a sample of SAMPLE_SIZE elements from each source.
		List<Cell<T>> sample = new ArrayList<>();
		for (List<Cell<T>> src : sources) {
			// step is src.size / SAMPLE_SIZE, rounded up to ensure step > 0
			int step = (src.size() + SAMPLE_SIZE - 1) / SAMPLE_SIZE;
			for (int i = 0; i < src.size(); i += step) {
				sample.add(src.get(i));
			}
		}

		Collections.sort(sample);

		List<ByteString> ret = new ArrayList<>();
		int nPartitions = sources.size();
		// Rounded up, to ensure step > 0
		int step = (sample.size() + nPartitions - 1) / nPartitions;
		for (int i = 0; i < sample.size(); i += step) {
			ret.add(sample.get(i).getRowKey());
		}
		assert ret.size() == sources.size() : "Should be " + sources.size() + " but was " + ret.size()
				+ " sample was " + sample.size();
		return ret;
	}

	private static <T> boolean shardsOrdered(Iterable<List<Cell<T>>> shards) {
		for (List<Cell<T>> s : shards) {
			if (!Ordering.natural().isOrdered(s)) {
				return false;
			}
		}

		return true;
	}

	private void printCounters() {
		Set<LocalCounter> counters = registry.get();
		synchronized (counters) { // Needed for iterating over a synchronized set.
			for (LocalCounter c : counters) {
				out.println(c.toString());
			}
		}
	}

	@Override
	public void close() throws IOException {
		threadPool.shutdownNow();
		try {
			threadPool.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt(); // Restore interrupted flag.
			return;
		}
	}
}