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
	final private Provider<Set<LocalCounter>> registry; // Provider for scoping.

	/**
	 * A snapshot of registry at the moment counter registered itself. Used to determine, when
	 * the current registration becomes invalid.
	 */
	private Set<LocalCounter> registeredAt;

	@Inject
	LocalCounter(@CounterName String counterName, @CounterLong Provider<AtomicLong> count,
		@CounterRegistry Provider<Set<LocalCounter>> registry) {
		this.counterName = checkNotNull(counterName);
		this.count = checkNotNull(count);
		this.registry = checkNotNull(registry);
	}

	@Override
	public void increment(long cnt) {
		// In a multi-threading situation, this could be executed multiply.
		// However, the registry is a synchronized set. It isn't incorrect or expensive:
		// Counter MAY register itself in the stale registry, but that will be fixed on next increment.
		Set<LocalCounter> curRegistry = registry.get();
		if (curRegistry != registeredAt) { // the old registration is invalid - need to update.
		    curRegistry.add(this);
		    registeredAt = curRegistry;
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