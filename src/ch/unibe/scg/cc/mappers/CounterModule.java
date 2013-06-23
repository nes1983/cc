package ch.unibe.scg.cc.mappers;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

/** CounterModule binds the counters to the M/R context. */
class CounterModule extends AbstractModule {
	final TaskAttemptContext context;

	public CounterModule(TaskAttemptContext context) {
		this.context = context;
	}

	@Override
	protected void configure() {
		bind(Counter.class).annotatedWith(Names.named(Constants.COUNTER_SUCCESSFULLY_HASHED)).toInstance(
				context.getCounter(Counters.SUCCESSFULLY_HASHED));
		bind(Counter.class).annotatedWith(Names.named(Constants.COUNTER_CANNOT_BE_HASHED)).toInstance(
				context.getCounter(Counters.CANNOT_BE_HASHED));
		bind(Counter.class).annotatedWith(Names.named(Constants.COUNTER_PROCESSED_FILES)).toInstance(
				context.getCounter(Counters.PROCESSED_FILES));
		bind(Counter.class).annotatedWith(Names.named(Constants.COUNTER_IGNORED_FILES)).toInstance(
				context.getCounter(Counters.IGNORED_FILES));
		bind(Counter.class).annotatedWith(Names.named(Constants.COUNTER_MAKE_FUNCTION_2_FINE_CLONES_ARRAY_EXCEPTIONS))
				.toInstance(context.getCounter(Counters.MAKE_FUNCTION_2_FINE_CLONES_ARRAY_EXCEPTIONS));
		bind(Counter.class).annotatedWith(Names.named(Constants.COUNTER_FUNCTIONS)).toInstance(
				context.getCounter(Counters.FUNCTIONS));
		bind(Counter.class).annotatedWith(Names.named(Constants.COUNTER_LOC)).toInstance(
				context.getCounter(Counters.LOC));
		bind(Counter.class).annotatedWith(Names.named(Constants.COUNTER_CLONES_REJECTED)).toInstance(
				context.getCounter(Counters.CLONES_REJECTED));
		bind(Counter.class).annotatedWith(Names.named(Constants.COUNTER_CLONES_PASSED)).toInstance(
				context.getCounter(Counters.CLONES_PASSED));
	}
}
