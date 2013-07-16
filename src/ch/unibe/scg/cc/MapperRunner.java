package ch.unibe.scg.cc;

import java.io.IOException;

import javax.inject.Provider;

/** MapperRunner launches a stage of a map/reduce pipeline. */
interface MapperRunner {
	/** Launch a stage of a map/reduce pipeline. Every mapper is guaranteed be launched in a separate thread. */
	<IN, OUT> void run(Provider<Mapper<IN, OUT>> map, CellSource<IN> src, CellSink<OUT> sink, Codec<IN> codecIn,
			Codec<OUT> codecOut) throws IOException;
}
