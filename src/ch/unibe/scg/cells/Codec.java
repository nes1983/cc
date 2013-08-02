package ch.unibe.scg.cells;

import java.io.IOException;


/**
 * A codec converts user-defined data types to and from cells.
 *
 * @author Niko Schwarz
 *
 * @param <T> The user-defined data type. Often a proto buffer.
 */
public interface Codec<T> {
	/** Encode {@code s} into a cell.*/
	Cell<T> encode(T s);
	/** Decode cell {@encoded}. */
	T decode(Cell<T> encoded) throws IOException;
}
