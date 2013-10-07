package ch.unibe.scg.cells;

import java.io.Closeable;
import java.io.Serializable;

/** A decoded {@link CellSource}. Neither iterable is guaranteed to be able to return more than one iterator. */
public interface Source<T> extends Iterable<Iterable<T>>, Serializable, Closeable {}
