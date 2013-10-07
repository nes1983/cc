package ch.unibe.scg.cells;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;


/**
 * Mapper is a stage of the pipeline. In standard Map/reduce terminology, this
 * is either a mapper or a reducer.
 *
 * <p>
 * Instances are NOT guaranteed to be thread-safe. It is permissible to write to
 * CellSinks that are instance variables. However, the main output is expected
 * to be written to the sink parameter of {@link #map(Object, OneShotIterable, Sink)}.
 *
 * @author Niko Schwarz
 *
 * @param <IN>
 *            Type of the input cells.
 * @param <OUT>
 *            Type of the output cells.
 */
public interface Mapper<IN, OUT> extends Closeable, Serializable {
	/**
	 * Map one input row, and write the main output to {@code sink}. <strong>The
	 * row is iterable only once.</strong>
	 *
	 * @param first
	 *            The first element of the row. This is useful to peek at an
	 *            element outside of iterating the row.
	 * @param row
	 *            Iterable only once. The first iterated element is
	 *            {@code first}.
	 */
	void map(IN first, OneShotIterable<IN> row, Sink<OUT> sink) throws IOException, InterruptedException;
}
