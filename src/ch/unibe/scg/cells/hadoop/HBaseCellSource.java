package ch.unibe.scg.cells.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import ch.unibe.scg.cells.Annotations.FamilyName;
import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.CellSource;

import com.google.common.collect.UnmodifiableIterator;
import com.google.protobuf.ByteString;

/** A cell source reading from a HTable. */
public class HBaseCellSource<T> implements CellSource<T> {
	static final private Logger logger = Logger.getLogger(HBaseCellSource.class);

	final private ByteString family;
	final private HTable hTable;


	@Inject
	HBaseCellSource(@FamilyName ByteString family, HTable hTable) {
		this.family = family;
		this.hTable = hTable;
	}

	/** Holds on to a closeable ResultScanner. Closes it at the end of iteration, and in finalizer. */
	class ResultScannerIterator extends UnmodifiableIterator<Iterable<Cell<T>>> {
		/** The current row's column keys and cell contents. Null if the iterator is empty. */
		Iterable<Entry<byte[], byte[]>> curRow;
		/** The current row's key. */
		ByteString rowKey;
		/** The source for the current iterator. */
		final ResultScanner scanner;

		/** {@code next} may be null. */
		ResultScannerIterator(ResultScanner scanner) {
			this.scanner = scanner;
			forward();
		}

		@Override public boolean hasNext() {
			return curRow != null;
		}

		@Override public Iterable<Cell<T>> next() {
			if (!hasNext()) { // Demanded by Iterator contract.
				throw new NoSuchElementException();
			}

			Iterable<Entry<byte[], byte[]>> oldRow = curRow;
			ByteString oldRowKey = rowKey;
			forward();

			List<Cell<T>> ret = new ArrayList<>();
			for (Entry<byte[], byte[]> e : oldRow) {
				ret.add(Cell.<T> make(oldRowKey, ByteString.copyFrom(e.getKey()), ByteString.copyFrom(e.getValue())));
			}
			return ret;
		}

		/** Advance curRow and rowKey to point to the row to be returned next. */
		private void forward() {
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
				curRow = r.getFamilyMap(family.toByteArray()).entrySet();
			}
		}

		@Override
		protected void finalize() throws Throwable {
			if (rowKey == null) {
				// We terminated normally.
				return;
			}
			try {
				scanner.close();
			} catch (Exception e) {
				logger.warn(e);
				throw e; // Will just be ignored.
			} finally {
				super.finalize();
			}
		}
	}

	/** Provides scans appropriate for reading tables sequentially, as in a Mapper. */
	private Scan makeScan() {
		Scan scan = new Scan();
		// HBase book says 500, see chapter 12.9.1. Hbase Book
		// This is 10 times faster than the default value.
		scan.setCaching(1000);

		scan.setCacheBlocks(false); // HBase book 12.9.5. Block Cache
		return scan;
	}

	@SuppressWarnings("resource") // Ressource closes itself on finalize (sorry ...)
	@Override
	public Iterator<Iterable<Cell<T>>> iterator() {
		Scan scan = makeScan();
		scan.addFamily(family.toByteArray());
		ResultScanner scanner;
		try {
			scanner = hTable.getScanner(scan);
		} catch (IOException e) {
			// Choose better exception.
			throw new RuntimeException(e);
		}
		return new ResultScannerIterator(scanner);
	}

	@Override
	public void close() throws IOException {
		hTable.close();
	}
}
