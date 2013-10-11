package ch.unibe.scg.cells;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;

import com.google.inject.BindingAnnotation;
import com.google.inject.Module;

/**
 * A module that enables support for counters. It's implementations must provide a binding
 * for {@link Counter}. The implementation, bound in this module, can expect a string,
 * annotated with {@link CounterName} to be available.
 **/
public interface CounterModule extends Module {
	/** Name of a counter. Names are unique to each counter. */
	@Retention(RUNTIME)
	@BindingAnnotation
	public static @interface CounterName {}
}