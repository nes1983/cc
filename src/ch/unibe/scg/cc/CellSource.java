package ch.unibe.scg.cc;

interface CellSource<T>  {
	/** @return all partitions. Neither iterable is guaranteed to be able to return more than one iterator. */
	Iterable<Iterable<Cell<T>>> partitions();
}
