package ch.unibe.scg.cc.modules;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import ch.unibe.scg.cc.Counters;
import ch.unibe.scg.cc.mappers.GuiceResource;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

/** CounterModule binds the counters to the M/R context. */
public class CounterModule extends AbstractModule {
	final TaskAttemptContext context;

	public CounterModule(TaskAttemptContext context) {
		this.context = context;
	}

	@Override
	protected void configure() {
		bind(Counter.class).annotatedWith(Names.named(GuiceResource.COUNTER_SUCCESSFULLY_HASHED)).toInstance(
				context.getCounter(Counters.SUCCESSFULLY_HASHED));
		bind(Counter.class).annotatedWith(Names.named(GuiceResource.COUNTER_CANNOT_BE_HASHED)).toInstance(
				context.getCounter(Counters.CANNOT_BE_HASHED));
		bind(Counter.class).annotatedWith(Names.named(GuiceResource.COUNTER_PROCESSED_FILES)).toInstance(
				context.getCounter(Counters.PROCESSED_FILES));
		bind(Counter.class).annotatedWith(Names.named(GuiceResource.COUNTER_IGNORED_FILES)).toInstance(
				context.getCounter(Counters.IGNORED_FILES));
		bind(Counter.class).annotatedWith(
				Names.named(GuiceResource.COUNTER_MAKE_FUNCTION_2_FINE_CLONES_ARRAY_EXCEPTIONS)).toInstance(
				context.getCounter(Counters.MAKE_FUNCTION_2_FINE_CLONES_ARRAY_EXCEPTIONS));
		bind(Counter.class).annotatedWith(Names.named(GuiceResource.COUNTER_FUNCTIONS)).toInstance(
				context.getCounter(Counters.FUNCTIONS));
		bind(Counter.class).annotatedWith(Names.named(GuiceResource.COUNTER_LOC)).toInstance(
				context.getCounter(Counters.LOC));
	}
}
