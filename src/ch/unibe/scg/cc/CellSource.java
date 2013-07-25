package ch.unibe.scg.cc;

/** iterator() returns all partitions. Neither iterable is guaranteed to be able to return more than one iterator. */
interface CellSource<T> extends Iterable<Iterable<Cell<T>>> {}
