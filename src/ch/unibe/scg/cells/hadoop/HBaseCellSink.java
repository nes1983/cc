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

import ch.unibe.scg.cells.Annotations.FamilyName;
import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.CellSink;
import ch.unibe.scg.cells.hadoop.Annotations.WriteToWalEnabled;

import com.google.protobuf.ByteString;

class HBaseCellSink<T> implements CellSink<T> {
	final private HTableWriteBuffer hTable;
	final private byte[] family;
	final private boolean writeToWalEnabled;

	/**
	 * Collects the puts in the buffer and writes them to HBase as soon as
	 * {@link #MAX_SIZE} is reached. Ensure to call {@link #close()} after the last
	 * Put got passed to the Buffer.
	 */
	private static class HTableWriteBuffer implements Closeable {
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
			htable.close();

			assert invariant();
		}
	}

	@Inject
	HBaseCellSink(HTableWriteBuffer hTable, @FamilyName ByteString family,
			@WriteToWalEnabled boolean writeToWalEnabled) {
		this.hTable = hTable;
		this.family = family.toByteArray();
		this.writeToWalEnabled = writeToWalEnabled;
	}

	@Override
	public void close() throws IOException {
		hTable.close();
	}

	@Override
	public void write(Cell<T> cell) throws IOException {
		Put put = new Put(cell.getRowKey().toByteArray());
		put.setWriteToWAL(writeToWalEnabled);
		put.add(family, cell.getColumnKey().toByteArray(), cell.getCellContents().toByteArray());
		hTable.write(put);
	}
}
