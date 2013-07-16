package ch.unibe.scg.cc;

import java.io.IOException;

import javax.inject.Provider;

import com.google.common.collect.Iterables;

// TODO: Run multithreaded
class InMemoryMapperRunner implements MapperRunner {
	@Override
	public <IN, OUT> void run(Provider<? extends Mapper<IN, OUT>> mapperProvider, CellSource<IN> src, CellSink<OUT> sink,
			Codec<IN> srcCodec, Codec<OUT> sinkCodec) throws IOException {
		try (Mapper<IN, OUT> mapper = mapperProvider.get()) {
			for (Iterable<Cell<IN>> part : src.partitions()) {
				Iterable<IN> decoded = Codecs.decode(part, srcCodec);
				// In memory, since all iterables are backed by arrays, this is safe.
				mapper.map(Iterables.get(decoded, 0), decoded, Codecs.encode(sink, sinkCodec));
			}
		} catch (EncodingException e) {
			throw e.getIOException();
		}
	}
}