package ch.unibe.scg.cells;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;
import javax.inject.Provider;

import ch.unibe.scg.cells.CounterModule.CounterLong;
import ch.unibe.scg.cells.CounterModule.CounterName;

/**
 * Represents a local {@link Counter}, that can be used inside {@link InMemoryPipeline}
 *
 * <p>
 * This class is {@link java.io.Serializable}, but there's a caveat.
 * Cells serializes this classes using {@link ShallowSerializingCopy}.
 * However, if serialization is attempted using a classical {@link java.io.ObjectOutputStream},
 * it will throw a {@link UnsupportedOperationException}.
 */
class LocalCounter implements Counter {
	private static final long serialVersionUID = 1L;

	final private String counterName;
	final private Provider<AtomicLong> count;
	final private Provider<Set<LocalCounter>> counterRegistry; // Provider for scoping.

	/** False if we've never registered this to counterRegistry; true otherwise. */
	private volatile boolean registered;

	@Inject
	LocalCounter(@CounterName String counterName, @CounterLong Provider<AtomicLong> count,
		@CounterRegistry Provider<Set<LocalCounter>> counterRegistry) {
		this.counterName = checkNotNull(counterName);
		this.count = checkNotNull(count);
		this.counterRegistry = checkNotNull(counterRegistry);
	}

	@Override
	public void increment(long cnt) {
		// In a multi-threading situation, this could be executed multiply.
		// However, the counterRegistry is a synchronized set. It isn't incorrect or expensive.
		if (!registered) {
			registered = true;
			counterRegistry.get().add(this);
		}

		count.get().addAndGet(cnt);
	}

	@Override
	public String toString() {
		//TODO: the full annotation name could be a little too much to display.
		return String.format("%s: %s", counterName, count.get());
	}

	private Object writeReplace() {
		return new ShallowSerializingCopy.SerializableLiveObject(this);
	}
}