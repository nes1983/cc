package ch.unibe.scg.cells.hadoop;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import ch.unibe.scg.cells.hadoop.Annotations.IndexFamily;
import ch.unibe.scg.cells.hadoop.Annotations.WriteToWalEnabled;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;

class HBaseCellSink<T> implements CellSink<T> {
	final static private long serialVersionUID = 1L;
	final static private byte[] EMPTY_BYTES = new byte[] {};

	private transient HTableWriteBuffer hTable;
	/** Do not modify. */
	final private byte[] family;
	/** Do not modify. */
	final private byte[] indexFamily;
	final private boolean writeToWalEnabled;
	final private HTableFactory tableFactory;

	@Inject
	HBaseCellSink(HTableWriteBuffer hTable, @FamilyName ByteString family, HTableFactory tableFactory,
			@WriteToWalEnabled boolean writeToWalEnabled, @IndexFamily ByteString indexFamily) {
		checkArgument(!family.equals(indexFamily), "Family names may not be " + indexFamily
				+ ". It's reserved for the index.");

		this.hTable = hTable;
		this.tableFactory = tableFactory;
		this.family = family.toByteArray();
		this.indexFamily = indexFamily.toByteArray();
		this.writeToWalEnabled = writeToWalEnabled;
	}

	private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();
		String tableName = stream.readUTF();
		hTable = new HTableWriteBuffer(tableFactory.make(tableName));
	}

	 private void writeObject(ObjectOutputStream out) throws IOException {
		 out.defaultWriteObject();
		 out.writeUTF(new String(hTable.htable.getTableName(), Charsets.UTF_8));
	 }

	/**
	 * Collects the puts in the buffer and writes them to HBase as soon as
	 * {@link #MAX_SIZE} is reached. Ensure to call {@link #close()} after the last
	 * Put got passed to the Buffer.
	 */
	private static class HTableWriteBuffer implements Closeable {
		private static final int MAX_SIZE = 10000;
		final private List<Row> puts = new ArrayList<>();
		final HTable htable;

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

	@Override
	public void close() throws IOException {
		hTable.close();
	}

	@Override
	public void write(Cell<T> cell) throws IOException {
		final byte[] rowKey = cell.getRowKey().toByteArray();
		final byte[] columnKey = cell.getColumnKey().toByteArray();

		Put cellPut = new Put(rowKey);
		cellPut.setWriteToWAL(writeToWalEnabled);
		cellPut.add(family, columnKey, cell.getCellContents().toByteArray());
		hTable.write(cellPut);

		Put indexPut = new Put(columnKey);
		indexPut.setWriteToWAL(writeToWalEnabled);
		indexPut.add(indexFamily, rowKey, EMPTY_BYTES);
		hTable.write(indexPut);
	}
}
