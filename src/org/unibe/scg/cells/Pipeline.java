package org.unibe.scg.cells;

import javax.inject.Provider;


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
 *  		.efflux(m2, codec3)
 * }
 */
public interface Pipeline<IN, EFF> {
	/** Return a new pipeline segment, ready for mapping. */
	MappablePipeline<IN, EFF> influx(Codec<IN> c);

	/** A segment that is shuffled and ready for mapping.  */
	public static interface MappablePipeline<I, EFF> {
		/** Set the mapper of influx {@code I} and efflux {@code E} */
		<E> ShuffleablePipeline<E, EFF> mapper(Provider<? extends Mapper<I, E>> m);
		/** Run the entire pipeline, with efflux encoding {@code codec}. This ends the pipeline.*/
		void efflux(Provider<? extends Mapper<I, EFF>> m, Codec<EFF> codec);
	}

	/** A segment that was just mapped and now needs shuffling or an efflux. */
	public interface ShuffleablePipeline<I, EFF> {
		/** Shuffle the efflux. This will also execute the previous mapper. */
		MappablePipeline<I, EFF> shuffle(Codec<I> codec);
	}
}