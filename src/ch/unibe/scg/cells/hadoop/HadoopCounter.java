package ch.unibe.scg.cells.hadoop;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.inject.Inject;

import ch.unibe.scg.cells.Counter;
import ch.unibe.scg.cells.CounterModule.CounterName;

/**
 * Represents a {@link Counter}, that uses a hadoop counter service to display information to user.
 * <p>
 * Cells serializes this classes using a specialized {@link HadoopPipeline.HadoopContextObjectInputStream}.
 * Otherwise, it will throw a {@link UnsupportedOperationException}.
 */
class HadoopCounter implements Counter {
	private static final long serialVersionUID = 1L;

	final private String counterName;
	private transient org.apache.hadoop.mapreduce.Counter counter;

	@Inject
	HadoopCounter(@CounterName String counterName) {
		this.counterName = checkNotNull(counterName);
	}

	@Override
	public void increment(long cnt) {
		if(counter != null) {
			counter.increment(cnt);
		} else {
			throw new IllegalStateException(String.format("Attempt to access to counter '%s' in an invalid context.", counterName));
		}
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		if (!(in instanceof HadoopPipeline.HadoopContextObjectInputStream)) {
			throw new UnsupportedOperationException(
					String.format("The class %s can only be serialized in a "
					+ HadoopPipeline.HadoopContextObjectInputStream.class.getName(), this.getClass().getName()));
		}

		HadoopPipeline.HadoopContextObjectInputStream hIn = (HadoopPipeline.HadoopContextObjectInputStream) in;
		hIn.defaultReadObject();
		this.counter = hIn.getCounter(counterName);
	}
}