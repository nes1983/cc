package ch.unibe.scg.cells;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.io.Closer;

/** Implementation of a {@link Pipeline} meant to run in memory. */
public class InMemoryPipeline<IN, OUT> implements Pipeline<IN, OUT> {
	final private CellSource<IN> pipeSrc;
	final private CellSink<OUT> pipeSink;

	/** Incredibly hacky way of moving an exception from one thread to another. */
	private static class ExceptionHolder {
		volatile Exception e; // volatile because it can be written and read from different threads.
	}

	private InMemoryPipeline(CellSource<IN> pipeSrc, CellSink<OUT> pipeSink) { // Don't subclass.
		this.pipeSrc = pipeSrc;
		this.pipeSink = pipeSink;
	}

	/** Create a pipeline. No parameters are allowed to be null. */
	public static <IN, OUT> InMemoryPipeline<IN, OUT> make(CellSource<IN> pipeSrc, CellSink<OUT> pipeSink) {
		checkNotNull(pipeSrc);
		checkNotNull(pipeSink);
		return new InMemoryPipeline<>(pipeSrc, pipeSink);
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
	private static <I, E> void run(CellSource<I> src, Codec<I> srcCodec, final Mapper<I, E> mapper,
			final CellSink<E> sink, final Codec<E> sinkCodec) throws IOException, InterruptedException {

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

			ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
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
					threadPool.awaitTermination(20, TimeUnit.SECONDS);
					Throwables.propagateIfPossible(cause, IOException.class, InterruptedException.class);
					throw new RuntimeException(cause);
				}

				if (waitGroup.get() == 0) {
					return;
				}

				Thread.sleep(70);
			}
		}
	}
}