package ch.unibe.scg.cc;

/** The output of a mapper is written into a sink */
interface Sink<T> {
	void write(T object);
}
