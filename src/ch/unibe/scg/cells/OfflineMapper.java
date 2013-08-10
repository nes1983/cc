package ch.unibe.scg.cells;

import java.io.Closeable;
import java.io.IOException;

/**
 * A mapper that reads the entire input into ram. It is executed only once, from one thread only.
 * It is legal for an OfflineMapper to fork more than one thread.
 */
public interface OfflineMapper<IN, OUT> extends Closeable {
	/** map the entire output from {@code in} into {@code out}. */
	void map(Source<IN> in, Sink<OUT> out) throws IOException, InterruptedException;
}
