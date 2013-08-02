package ch.unibe.scg.cells;

/** iterator() returns all partitions. Neither iterable is guaranteed to be able to return more than one iterator. */
public interface CellSource<T> extends Iterable<Iterable<Cell<T>>> {}
