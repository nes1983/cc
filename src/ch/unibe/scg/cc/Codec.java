package ch.unibe.scg.cc;

import java.io.IOException;

/**
 * A codec converts user-defined data types to and from cells.
 *
 * @author Niko Schwarz
 *
 * @param <T> The user-defined data type. Often a proto buffer.
 */
interface Codec<T> {
	Cell<T> encode(T s);
	T decode(Cell<T> encoded) throws IOException;
}
