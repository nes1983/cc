package ch.unibe.scg.cells.hadoop;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.HTable;
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
	/** May be null! */
	final private SerializableHTable hTable;
	private transient ResultScanner scanner;

	@Inject
	HBaseCellSource(@FamilyName ByteString family, SerializableHTable hTable) {
		// Doing openScanner in the object and not in the provider drastically
		// simplifies serialization.
		this(family, openScanner(hTable.hTable, family.toByteArray()), hTable);
	}

	HBaseCellSource(@FamilyName ByteString family, ResultScanner scanner, SerializableHTable hTable) {
		this.family = family.toByteArray();
		this.scanner = scanner;
		this.hTable = hTable;
	}

	private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
		in.defaultReadObject();
		scanner = openScanner(hTable.hTable, family);
	}

	private class ResultScannerIterator extends UnmodifiableIterator<Cell<T>> implements Closeable {
		/** The current row's column keys and cell contents. Null if the iterator is empty. */
		Iterable<Entry<byte[], byte[]>> curRow;
		/** The current row's key. */
		ByteString rowKey;

		/** {@code next} may be null. */
		ResultScannerIterator() {
			readNextRow();
		}

		@Override public boolean hasNext() {
			return curRow != null;
		}

		@Override public Cell<T> next() {
			if (!hasNext()) { // Demanded by Iterator contract.
				throw new NoSuchElementException();
			}

			Entry<byte[], byte[]> c = curRow.iterator().next();
			Cell<T> ret = Cell.<T> make(rowKey,
					ByteString.copyFrom(c.getKey()), ByteString.copyFrom(c.getValue()));

			if (nextRowNeeded()) {
				readNextRow();
			}

			return ret;
		}

		@Override public void close() {
			scanner.close();
		}

		/** Advance curRow and rowKey to point to the row to be returned next. */
		private void readNextRow() {
			Result r;
			try {
				r = scanner.next();
			} catch (IOException e1) {
				// TODO: Choose a better exception.
				throw new RuntimeException(e1);
			}
			if (r == null) {
				rowKey = null;
				curRow = null;
				scanner.close();
			} else {
				rowKey = ByteString.copyFrom(r.getRow());
				curRow = r.getFamilyMap(family).entrySet();
			}
		}

		private boolean nextRowNeeded() {
			return curRow == null || !curRow.iterator().hasNext();
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

	private final static ResultScanner openScanner(HTable tab, byte[] family) {
		Scan scan = makeScan();
		scan.addFamily(family);
		try {
			return tab.getScanner(scan);
		} catch (IOException e) {
			throw new RuntimeException("Couldn't open table " + new String(tab.getTableName(), Charsets.UTF_8), e);
		}
	}

	@Override
	public void close() throws IOException {
		try (Closer closer = Closer.create()) {
			if (hTable != null) {
				closer.register(hTable.hTable);
			}
			closer.register(scanner);
		}
	}

	@Override
	public int nShards() {
		try (ResultScannerIterator r = new ResultScannerIterator()) {
			boolean isEmpty = r.hasNext();
			if (isEmpty) {
				return 0;
			}

			return 1;
		}
	}

	@Override
	public OneShotIterable<Cell<T>> getShard(int shard) {
		checkArgument(shard >= 0, "Shard number cannot be negative");
		checkArgument(shard < nShards(), "Shard number cannot be greater than %s", nShards());

		return new AdapterOneShotIterable<>(new ResultScannerIterator());
	}
}
