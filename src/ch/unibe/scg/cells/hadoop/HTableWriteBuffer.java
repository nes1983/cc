package ch.unibe.scg.cells.hadoop;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;


/**
 * Collects the puts in the buffer and writes them to HBase as soon as
 * {@link #MAX_SIZE} is reached. Ensure to call {@link #close()} after the last
 * Put got passed to the Buffer.
 */
class HTableWriteBuffer implements Closeable {
	private static final int MAX_SIZE = 10000;
	final private List<Row> puts = new ArrayList<>();
	final private HTable htable;

	@Inject
	HTableWriteBuffer(HTable htable) {
		this.htable = htable;
	}

	/** Write put into table. */
	public void write(Put put) throws IOException {
		checkNotNull(put);
		puts.add(put);
		if (puts.size() == MAX_SIZE) {
			HTableUtil.bucketRsBatch(htable, puts);
			puts.clear();
		}
		assert invariant();
	}

	private void writeRemainingPuts() throws IOException {
		HTableUtil.bucketRsBatch(htable, puts);
		assert invariant();
	}

	protected boolean invariant() {
		return puts.size() <= MAX_SIZE;
	}

	/** Writes the remaining puts. */
	@Override
	public void close() throws IOException {
		writeRemainingPuts();
		assert invariant();
	}
}