package ch.unibe.scg.cc;

import java.io.IOException;

import javax.inject.Provider;

// TODO: Run multithreaded
class InMemoryMapperRunner implements MapperRunner {
	@Override
	public <IN, OUT> void run(Provider<Mapper<IN, OUT>> mapperProvider, CellSource<IN> src, CellSink<OUT> sink,
			Codec<IN> srcCodec, Codec<OUT> sinkCodec) throws IOException {
		try (Mapper<IN, OUT> mapper = mapperProvider.get()) {
			for (Iterable<Cell<IN>> part : src.partitions()) {
				mapper.map(Codecs.decode(part, srcCodec), Codecs.encode(sink, sinkCodec));
			}
		} catch (EncodingException e) {
			throw e.getIOException();
		}
	}
}