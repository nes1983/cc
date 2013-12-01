package ch.unibe.scg.cells;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;
import javax.inject.Provider;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.io.Closer;
import com.google.protobuf.ByteString;

/** Implementation of a {@link Pipeline} meant to run in memory. */
public class InMemoryPipeline<IN, OUT> implements Pipeline<IN, OUT> {
	final private static int PRINT_INTERVAL = 1; // in seconds.
	final private static int SHUTDOWN_TIMEOUT = 20; // in seconds.
	final private static int SAMPLE_SIZE = 100; // How much to sample from each shard to get splitters.
	final private CellSource<IN> pipeSrc;
	final private CellSink<OUT> pipeSink;
	final private PipelineStageScope scope;
	final private Provider<Set<LocalCounter>> registry;
	final private PrintStream out;

	/** Incredibly hacky way of moving an exception from one thread to another. */
	private static class ExceptionHolder {
		volatile Exception e; // volatile because it can be written and read from different threads.
	}

	InMemoryPipeline(CellSource<IN> pipeSrc, CellSink<OUT> pipeSink, PipelineStageScope scope,
			Provider<Set<LocalCounter>> counterRegistry, PrintStream out) { // Don't subclass.
		this.pipeSrc = pipeSrc;
		this.pipeSink = pipeSink;
		this.scope = scope;
		this.registry = counterRegistry;
		this.out = out;
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
		public <IN, OUT> InMemoryPipeline<IN, OUT> make(CellSource<IN> pipeSrc, CellSink<OUT> pipeSink) {
			checkNotNull(pipeSrc);
			checkNotNull(pipeSink);
			// counter info is diagnostic information, not actual output.
			return new InMemoryPipeline<>(pipeSrc, pipeSink, scope, registry, System.err);
		}
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
			run(src, srcCodec, m, pipeSink, sinkCodec);
			pipeSink.close();
		}

		@Override
		public void effluxWithOfflineMapper(OfflineMapper<I, OUT> offlineMapper, Codec<OUT> codec)
				throws IOException, InterruptedException {
			try(OfflineMapper<I, OUT> m = offlineMapper) {
				// Rather than close the sinks, we close the underlying CellSink directly.
				m.map(Codecs.decode(src, srcCodec), Codecs.encode(pipeSink, codec));
			}
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
			try (InMemoryShuffler<E> shuffler = new InMemoryShuffler<>()) {
				run(src, srcCodec, mapper, shuffler, sinkCodec);
				return new InMemoryMappablePipeline<>(shuffler, sinkCodec);
			}
		}
	}

	/**
	 * Run the mapper. Neither source nor sink are closed. However, mapper gets closed.
	 * If an exception occurs in any mapper, it is printed to stderr from within that thread.
	 * Any one of the exceptions is picked and returned to the caller of this method.
	 * The threadpool gets shut down on exceptions, too.
	 */
	// TODO: Perhaps report all exceptions as suppressed?
	private <I, E> void run(CellSource<I> src, Codec<I> srcCodec, final Mapper<I, E> mapper,
			final CellSink<E> sink, final Codec<E> sinkCodec) throws IOException, InterruptedException {
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
		ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

		try(final Closer mapperCloser = Closer.create()) {
			mapperCloser.register(mapper);

			// If an exception occurs, write it in here. The indirection is necessary, because inner classes
			// cannot write to local variables.
			final ExceptionHolder exceptionHolder = new ExceptionHolder();

			final Object mapperCloserLock = new Object(); // Controls access to mapperCloser.

			// Returns null on IOException, which will be dealt with by the end of the method.
			final ThreadLocal<Mapper<I, E>> localMapper = new ThreadLocal<Mapper<I, E>>() {
				@Override protected Mapper<I, E> initialValue() {
					Mapper<I, E> cloned;
					try {
						cloned = ShallowSerializingCopy.clone(mapper);
					} catch (IOException e) {
						exceptionHolder.e = e;
						System.err.println("Suppressed exception:");
						e.printStackTrace();
						return null;
					}
					synchronized(mapperCloserLock) {
						mapperCloser.register(cloned);
					}
					return cloned;
				}
			};

			Runnable run = new Runnable() {
			    @Override public void run() {
			         printCounters();
			   }
			};

			scheduler.scheduleAtFixedRate(run, PRINT_INTERVAL, PRINT_INTERVAL, TimeUnit.SECONDS);
			final AtomicInteger waitGroup = new AtomicInteger();
			// TODO: move decoding into worker thread.
			for (Iterable<I> decoded : Codecs.decode(src, srcCodec)) {
				waitGroup.incrementAndGet();
				Iterator<I> iter = decoded.iterator();
				final I first = iter.next();
				final Iterable<I> row = Iterables.concat(Arrays.asList(first), new AdapterOneShotIterable<>(iter));
				threadPool.execute(new Runnable() {
					@Override public void run() {
						try {
							@SuppressWarnings("resource") // gets closed by the closer.
							Mapper<I, E> m = localMapper.get();
							if (m == null) {
								return;
							}
							m.map(first, new AdapterOneShotIterable<>(row), Codecs.encode(sink, sinkCodec));
						} catch (Exception e) {
							exceptionHolder.e = e;
							System.err.println("Suppressed exception:");
							e.printStackTrace();
							return;
						}

						waitGroup.decrementAndGet();
					}
				});
			}
			while (true) {
				if (exceptionHolder.e != null) {
					Exception cause = exceptionHolder.e; // Don't let anybody overwrite the exception while handling.
					threadPool.shutdownNow();
					threadPool.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);

					scheduler.shutdownNow();
					scheduler.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
					Throwables.propagateIfPossible(cause, IOException.class, InterruptedException.class);
					throw new RuntimeException(cause);
				}

				if (waitGroup.get() == 0) {
					return;
				}

				Thread.sleep(70);
			}
		} finally {
			if (scope != null) {
				// to ensure counters will be printed upon completion.
				printCounters();
				scope.exit();

				// shutdown executors: their threads will prevent application shutdown.
				threadPool.shutdownNow();
				threadPool.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);

				scheduler.shutdownNow();
				scheduler.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
			}
		}
	}

	/**
	 * @return the sorted list of all entries in {@code sources} between {@code from} and {@code to}.
	 * @param sources collection of individually sorted lists.
	 * @param from first row key to be included, inclusive.
	 * @param to last row key to be included, exclusive.
	 */
	static <T> List<Cell<T>> merge(Iterable<List<Cell<T>>> sources, ByteString from, ByteString to) {
		assert shardsOrdered(sources);

		Cell<T> fromProbe = Cell.make(from, ByteString.EMPTY, ByteString.EMPTY);
		Cell<T> toProbe = Cell.make(to, ByteString.EMPTY, ByteString.EMPTY);

		Set<Cell<T>> ret = new HashSet<>();
		for (List<Cell<T>> src : sources) {
			int fromPos = insertionPoint(fromProbe, src);
			int toPos = insertionPoint(toProbe, src);

			ret.addAll(src.subList(fromPos, toPos));
		}

		return Ordering.natural().immutableSortedCopy(ret);
	}

	/** Same as {@link #merge}, but assuming parameter {@code to} as infinite. */
	static <T> List<Cell<T>> mergeUnbounded(Iterable<List<Cell<T>>> sources, ByteString from) {
		assert shardsOrdered(sources);

		Cell<T> fromProbe = Cell.make(from, ByteString.EMPTY, ByteString.EMPTY);

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
	 * @param sources each source should be sorted.
	 */
	static <T> List<ByteString> splitters(Iterable<List<Cell<T>>> sources, int nPartitions) {
		assert nPartitions > 0;
		assert shardsOrdered(sources);

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
		// Rounded up, to ensure step > 0
		int step = sample.size() + nPartitions - 1 / nPartitions;
		for (int i = 0; i < sample.size(); i += step) {
			ret.add(sample.get(i).getRowKey());
		}
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
		synchronized (registry) {
			for(LocalCounter c : registry.get()) {
				if(Thread.interrupted()) {
					// restoring interrupted flag, as this method could be called outside of executor's pool.
					Thread.currentThread().interrupt();
					break;
				}

				out.println(c.toString());
			}
		}
	}
}