package ch.unibe.scg.cc;

import java.io.IOException;

import javax.inject.Provider;

// TODO: Run multithreaded
class InMemoryMapperRunner implements MapperRunner {
	@Override
	public <IN, OUT> void run(Provider<Mapper<IN, OUT>> mapperProvider, CellSource<IN> src, CellSink<OUT> sink) throws IOException {
		Mapper<IN, OUT> mapper = mapperProvider.get();
		for (Iterable<Cell<IN>> part : src.partitions()) {
			mapper.map(part, sink);
		}
	}
}