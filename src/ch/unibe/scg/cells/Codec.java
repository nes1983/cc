package ch.unibe.scg.cells;

import java.io.IOException;
import java.io.Serializable;


/**
 * A codec converts objects of type {@code T} to and from cells.
 *
 * <p>There can be more than one Codec for the same type {@code T}.
 * However, a cell should be decoded by the same codec that encoded it.
 *
 * @param <T> The user-defined data type.
 * @see Cell
 */
public interface Codec<T> extends Serializable {
	/** Encode {@code obj} into a cell.*/
	Cell<T> encode(T obj);
	/** Decode cell. */
	T decode(Cell<T> cell) throws IOException;
}
