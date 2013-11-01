package ch.unibe.scg.cells.hadoop;

import ch.unibe.scg.cells.Counter;
import ch.unibe.scg.cells.CounterModule;

import com.google.inject.AbstractModule;

/**
 * Counters that are shared across machines.
 * Hadoop counter service is used to display information to user.
 */
public final class HadoopCounterModule extends AbstractModule implements CounterModule {
	@Override
	protected void configure() {
		bind(Counter.class).to(HadoopCounter.class);
	}
}