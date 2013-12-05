package ch.unibe.scg.cells.hadoop;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
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
	/** May be null! */
	final private SerializableHTable hTable;
	private transient Closer closer = Closer.create();

	private class ResultScannerIterator extends UnmodifiableIterator<Cell<T>> implements Closeable {
		final private ResultScanner scanner;

		/** The current row's column keys and cell contents. Null if the iterator is empty. */
		private Iterable<Entry<byte[], byte[]>> curRow;
		/** The current row's key. */
		private ByteString rowKey;

		/** {@code next} may be null. */
		ResultScannerIterator(ResultScanner scanner) {
			this.scanner = scanner;
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
				throw new RuntimeException(e1); // unchecked because iterator cannot throw
			}
			if (r == null) {
				rowKey = null;
				curRow = null;
				close(); // closing here because iterator is exhausted. Faster than relying on source's close.
			} else {
				rowKey = ByteString.copyFrom(r.getRow());
				curRow = r.getFamilyMap(family).entrySet();
			}
		}

		private boolean nextRowNeeded() {
			return curRow == null || !curRow.iterator().hasNext();
		}
	}

	@Inject
	HBaseCellSource(@FamilyName ByteString family, SerializableHTable hTable) {
		this.family = family.toByteArray();
		this.hTable = hTable;
	}

	private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
		in.defaultReadObject();
		closer = Closer.create();
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

	private final ResultScanner openScanner() {
		Scan scan = makeScan();
		scan.addFamily(family);
		try {
			ResultScanner ret = hTable.hTable.getScanner(scan);
			closer.register(ret);
			return ret;
		} catch (IOException e) {
			throw new RuntimeException("Couldn't open table " + new String(hTable.hTable.getTableName(), Charsets.UTF_8), e);
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
	public int nShards() {
		try (ResultScanner sc = openScanner();
				ResultScannerIterator r = new ResultScannerIterator(sc)) {
			boolean isEmpty = r.hasNext();
			if (isEmpty) {
				return 0;
			}

			return 1;
		}
	}

	@SuppressWarnings("resource") // scanner will get closed upon closing the source.
	@Override
	public OneShotIterable<Cell<T>> getShard(int shard) {
		if (shard < 0 || nShards() <= shard) {
			throw new IndexOutOfBoundsException(
					String.format("You asked for shard %s, but there are only %s.", shard, nShards()));
		}

		return new AdapterOneShotIterable<>(new ResultScannerIterator(openScanner()));
	}
}
