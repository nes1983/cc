package ch.unibe.scg.cells;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.inject.Qualifier;

import com.google.inject.Module;

/**
 * A module that enables support for counters. It's implementations must provide a binding
 * for {@link Counter}. The implementation, bound in this module, can expect a string,
 * annotated with {@link CounterName} to be available.
 **/
public interface CounterModule extends Module {
	/** Name of a counter. Names are unique to each counter. */
	@Qualifier
	@Target({ FIELD, PARAMETER, METHOD })
	@Retention(RUNTIME)
	public static @interface CounterName {}

	/** The backing AtomicLong to be used in {@link Counter}s. */
	@Qualifier
	@Target({ FIELD, PARAMETER, METHOD })
	@Retention(RUNTIME)
	static @interface CounterLong {}
}