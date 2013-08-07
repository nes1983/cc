package ch.unibe.scg.cells;

import java.io.Closeable;

/** iterator() returns all partitions. Neither iterable is guaranteed to be able to return more than one iterator. */
public interface CellSource<T> extends Iterable<Iterable<Cell<T>>>, Closeable {}
// TODO: having CellSource EXTEND Iterable may be convenient, but it forces finalizers down the pipe.
// Change this. Seriously!