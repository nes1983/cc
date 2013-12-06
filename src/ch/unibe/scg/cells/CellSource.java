package ch.unibe.scg.cells;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

/**
 * A sharded source of cells.
 * A shard is a {@link OneShotIterable} of cells that will be processed single-threaded.
 * Cells must be sorted inside shards, and the first element of any shard must be
 * greater than the last cell in the previous shard. This implies that sharding
 * may not split rows in two, that is: all elements of a row must always be in the same
 * same shard.
 *
 * <p> Shard are not allowed to be empty; thus nShards() can result zero, in case if there no data.
 *
 * <p> After source is closed, it is no longer possible to get shards from it and
 * all shards obtained from it can no longer be iterated.
 *
 * <p>Implementers should choose the size of each shard big enough so scheduling shards
 * does not become the bottle-neck during parallel processing. On the other hand,
 * it should be small enough to have enough shards to distribute evenly over all processors.
 *
 * <p>Note that a shard is <em>not</em> the same as a row. A row is a set of cells
 * that share the same row key. A shard can contain more than one row.
 */
public interface CellSource<T> extends Closeable, Serializable {
	/** @return number of shards. */
	int nShards() throws IOException;
	/** @return the <i>i</i>th shard. */
	OneShotIterable<Cell<T>> getShard(int shard) throws IOException;
}