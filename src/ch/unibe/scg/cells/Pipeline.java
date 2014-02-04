package ch.unibe.scg.cells;

import java.io.IOException;

/**
 * Pipeline is the API that should be used to specify the control flow of a map reduce job.
 * Side outputs are currently unsupported, but will soon be added.
 *
 * <p>
 * Example:
 * {@code
 * 		Pipeline pipe = new InMemoryPipeline(source, sink);
 *  	pipe
 *  		.influx(codec1)
 *  		.mapper(m1)
 *  		.shuffle(codec2)
 *  		.mapAndEfflux(m2, codec3) 
 * }
 */
// TODO: Give a longer example.

public interface Pipeline<IN, EFF> {
	/** Return a new pipeline segment, ready for mapping. */
	MappablePipeline<IN, EFF> influx(Codec<IN> c);

	/** @return result of the last pipeline run. */
	Source<EFF> lastEfflux();

	/** A segment that is shuffled and ready for mapping.  */
	public static interface MappablePipeline<I, EFF> {
		/** Set the mapper of influx {@code I} and efflux {@code E} */
		<E> ShuffleablePipeline<E, EFF> map(Mapper<I, E> m);
		/**
		 * Run the entire pipeline, with efflux encoding {@code codec}. This ends the pipeline.
		 * @throws IOException If several exceptions occur, any may be reported.
		 */
		void mapAndEfflux(Mapper<I, EFF> m, Codec<EFF> codec) throws IOException, InterruptedException;
	}

	/** A segment that was just mapped and now needs shuffling or an efflux. */
	public interface ShuffleablePipeline<I, EFF> {
		/**
		 * Shuffle the efflux. This will also execute the previous mapper.
		 * @throws IOException  If several exceptions occur, any may be reported.
		 */
		MappablePipeline<I, EFF> shuffle(Codec<I> codec) throws IOException, InterruptedException;
	}
}
