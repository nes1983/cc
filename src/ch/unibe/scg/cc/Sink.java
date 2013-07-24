package ch.unibe.scg.cc;

/** The output of a mapper is written into a sink */
// TODO: Should this be closeable?
public interface Sink<T> {
	/** Write {@code object} into the sink. */
	void write(T object);
}
