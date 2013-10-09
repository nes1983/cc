package ch.unibe.scg.cells;

import java.io.Serializable;

/**
 * A counter that will be displayed to the user by cells.
 * There is no API to get the value of the counter. In the case of the in memory pipeline,
 * values are displayed in the console. In the case of Hadoop counters, hadoop will display
 * the values on its own monitoring web service.
 *
 * <p>
 * All methods must be thread-safe.
 *
 * <p>
 * Note that this counter is unlike a Hadoop counter, in that it isn't only usable from one
 * mapper. This counter is serializable and usable across mappers.
 * However, if the same counter is used from different stages in a pipeline, i.e.
 * from different mappers, the counts may NOT carry over from one stage to another.
 */
public interface Counter extends Serializable {
	/** Increase the counter by {@code cnt}. */
	public void increment(long cnt);
}
