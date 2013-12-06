package ch.unibe.scg.cells.hadoop;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import ch.unibe.scg.cells.AdapterOneShotIterable;
import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.CellSource;
import ch.unibe.scg.cells.OneShotIterable;
import ch.unibe.scg.cells.hadoop.HBaseStorage.FamilyName;

import com.google.common.base.Charsets;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.io.Closer;
import com.google.protobuf.ByteString;

/** A cell source reading from a HTable. Don't forget to close it when you're done. */
public class HBaseCellSource<T> implements CellSource<T>{
	final private static long serialVersionUID = 1L;

	/** Do not modify. */
	final private byte[] family;
	final private SerializableHTable hTable;
	private transient Closer closer = Closer.create();

	@Inject
	HBaseCellSource(@FamilyName ByteString family, SerializableHTable hTable) {
		this.family = family.toByteArray();
		this.hTable = hTable;
	}

	private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
		in.defaultReadObject();
		closer = Closer.create();
	}

	private class ResultScannerIterator extends UnmodifiableIterator<Cell<T>> {
		/** The current row's column keys and cell contents. */
		private Iterator<Entry<byte[], byte[]>> curRow;

		/** The current row's key. */
		private ByteString curRowKey;

		final private Iterator<Result> nextRows;


		/** {@code next} may be null. */
		ResultScannerIterator(Iterator<Result> nextRows) {
			this.nextRows = nextRows;
		}

		@Override
		public boolean hasNext() {
			if (curRow != null && curRow.hasNext()) {
				return true;
			}

			return nextRows.hasNext();
		}

		@Override
		public Cell<T> next() {
			if (!hasNext()) { // Demanded by Iterator contract.
				throw new NoSuchElementException();
			}

			if (curRow == null || !curRow.hasNext()) {
				// Read next row.
				Result r = nextRows.next();
				curRowKey = ByteString.copyFrom(r.getRow());
				curRow = r.getFamilyMap(family).entrySet().iterator();
			}

			Entry<byte[], byte[]> c = curRow.next();

			return Cell.<T> make(curRowKey,
					ByteString.copyFrom(c.getKey()),
					ByteString.copyFrom(c.getValue()));
		}
	}

	/** Provides scans appropriate for reading tables sequentially, as in a Mapper. */
	static Scan makeScan() {
		Scan scan = new Scan();
		// HBase book says 500, see chapter 12.9.1. Hbase Book
		// This is 10 times faster than the default value.
		scan.setCaching(1000);

		scan.setCacheBlocks(false); // HBase book 12.9.5. Block Cache
		return scan;
	}

	private ResultScanner openScanner() throws IOException {
		Scan scan = makeScan();
		scan.addFamily(family);
		try {
			return hTable.hTable.getScanner(scan);
		} catch (IOException e) {
			throw new IOException("Couldn't open table "
					+ new String(hTable.hTable.getTableName(), Charsets.UTF_8), e);
		}
	}

	@Override
	public void close() throws IOException {
		if (hTable != null) {
			closer.register(hTable.hTable);
		}

		closer.close();
	}

	@Override
	public int nShards() throws IOException {
		try (ResultScanner scan = openScanner()) {
			if (scan.iterator().hasNext()) {
				return 1;
			}

			return 0;
		}
	}

	@SuppressWarnings("resource") // scan is added to the closer, and therefore closed.
	@Override
	public OneShotIterable<Cell<T>> getShard(int shard) throws IOException {
		if (shard < 0 || nShards() <= shard) {
			throw new IndexOutOfBoundsException(
					String.format("You asked for shard %s, but there are only %s.", shard, nShards()));
		}

		ResultScanner scan = openScanner();
		closer.register(scan);

		return new AdapterOneShotIterable<>(new ResultScannerIterator(scan.iterator()));
	}
}
