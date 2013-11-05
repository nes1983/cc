package ch.unibe.scg.cells;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.inject.Scope;

/**
 * The collection that contains all counters. Threadsafe over adding and removing,
 * but needs explicit synchronization for iteration, as described in
 * {@link java.util.Collections#synchronizedSet(java.util.Set)}.
 * This is a set, so re-adding an existing counter again has no effect.
 */
@Target({ FIELD, PARAMETER, METHOD })
@Retention(RUNTIME)
@Scope
@interface CounterRegistry { }