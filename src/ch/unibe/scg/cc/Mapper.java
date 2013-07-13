package ch.unibe.scg.cc;

import java.io.IOException;

/**
 * Mapper is a stage of the pipeline. In standard Map/reduce terminology, this
 * is either a mapper or a reducer.
 *
 * <p>
 * Instances are NOT guaranteed to be thread-safe. It is permissible to write to
 * CellSinks that are instance variables. However, the main output is expected
 * to be written to the sink parameter of {@link #map()}.
 *
 * @author Niko Schwarz
 *
 * @param <IN>
 *            Type of the input cells.
 * @param <OUT>
 *            Type of the output cells.
 */
interface Mapper<IN, OUT> {
	/** Map one input row, and write the main output to {@code sink} */
	void map(Iterable<Cell<IN>> row, CellSink<OUT> sink) throws IOException;
}
