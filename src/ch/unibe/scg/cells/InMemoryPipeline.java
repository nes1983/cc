package ch.unibe.scg.cells;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import javax.inject.Provider;

import com.google.common.collect.Iterables;

// TODO: Run multithreaded
/** Implementation of a {@link Pipeline} meant to run in memory. */
public class InMemoryPipeline<IN, OUT> implements Pipeline<IN, OUT> {
	final private CellSource<IN> pipeSrc;
	final private CellSink<OUT> pipeSink;

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
		public <E> ShuffleablePipeline<E, OUT> mapper(Provider<? extends Mapper<I, E>> mapper) {
			return new InMemoryShuffleablePipeline<>(src, srcCodec, mapper);
		}

		@Override
		public void efflux(Provider<? extends Mapper<I, OUT>> m, Codec<OUT> sinkCodec)
				throws IOException, InterruptedException {
			run(src, srcCodec, m, pipeSink, sinkCodec);
			pipeSink.close();
		}

		@Override
		public void effluxWithOfflineMapper(Provider<? extends OfflineMapper<I, OUT>> offlineMapper, Codec<OUT> codec)
				throws IOException, InterruptedException {
			try(OfflineMapper<I, OUT> m = offlineMapper.get()) {
				// Rather than close the sinks, we close the underlying CellSink directly.
				m.map(Codecs.decode(src, srcCodec), Codecs.encode(pipeSink, codec));
			}
		}
	}

	private class InMemoryShuffleablePipeline<I, E> implements ShuffleablePipeline<E, OUT> {
		final private CellSource<I> src;
		final private Codec<I> srcCodec;
		final private Provider<? extends Mapper<I, E>> mapper;

		InMemoryShuffleablePipeline(CellSource<I> src, Codec<I> srcCodec, Provider<? extends Mapper<I, E>> mapper) {
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

	/** Run the mapper and close the sink.
	 * @throws InterruptedException */
	public static <I, E> void run(CellSource<I> src, Codec<I> srcCodec, Provider<? extends Mapper<I, E>> mapper,
			CellSink<E> sink, Codec<E> sinkCodec) throws IOException, InterruptedException {
		try (Mapper<I, E> m = mapper.get()) {
			// Rather than close the sinks, we close the underlying CellSink directly.
			Source<I> decodedSrc = Codecs.decode(src, srcCodec);
			for (Iterable<I> decoded : decodedSrc) {
				Iterator<I> iter = decoded.iterator();
				I first = iter.next();
				Iterable<I> row = Iterables.concat(Arrays.asList(first), new OneShotIterable<>(iter));
				m.map(first, row, Codecs.encode(sink, sinkCodec));
			}
		}
	}
}