package ch.unibe.scg.cc;

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
 *  		.mapper(m2)
 *  		.efflux(codec3)
 *  		.run()}
 */
public interface Pipeline {
	/** Return a new pipeline segment, ready for mapping. */
	<I> MappablePipeline<I> influx(Codec<I> c);

	/** A segment that is shuffled and ready for mapping.  */
	public static interface MappablePipeline<I> {
		/** Set the mapper of influx {@code I} and efflux {@code E} */
		public <E> ShuffleablePipeline<E> mapper(Provider<Mapper<I, E>> m);
	}

	/** A segment that was just mapped and now needs shuffling or an efflux. */
	public static interface ShuffleablePipeline<I> {
		/** Shuffle the efflux. This will also execute the previous mapper. */
		MappablePipeline<I> shuffle(Codec<I> codec);

		/** Run the entire pipeline, with efflux encoding {@code codec}. This ends the pipeline.*/
		RunnablePipeline efflux(Codec<I> codec);
	}

	/** Run the pipeline after construction */
	public static interface RunnablePipeline extends Runnable {}
}