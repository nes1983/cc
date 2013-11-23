package ch.unibe.scg.cells.hadoop;

import java.io.IOException;
import java.util.Map.Entry;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.CellLookupTable;
import ch.unibe.scg.cells.hadoop.HBaseStorage.FamilyName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.protobuf.ByteString;

/** Optimized for small row reads. */
class HBaseCellLookupTable<T> implements CellLookupTable<T> {
	final private static long serialVersionUID = 1L;

	final private SerializableHTable hTable;
	/** Do not modify. */
	final private byte[] family;

	@Inject
	HBaseCellLookupTable(SerializableHTable hTable, @FamilyName ByteString family) {
		this.hTable = hTable;
		this.family = family.toByteArray();
	}

	@Override
	public Iterable<Cell<T>> readRow(ByteString rowPrefix) throws IOException {
		byte[] from = rowPrefix.toByteArray();

		// `To` is minimally greater than from, if possible.
		byte[] to = rowPrefix.toByteArray();
		boolean toGreaterThanFrom = false;
		for (int i = from.length - 1; i >= 0; i--) {
			if (to[i] != Byte.MAX_VALUE) {
				to[i]++;
				toGreaterThanFrom = true;
				break;
			}
		}

		Scan scan;
		if (toGreaterThanFrom) {
			scan = new Scan(from, to); // Range: [from, to)
		} else {
			scan = new Scan(from); // Range: [from, inf)
		}
		scan.addFamily(family);


		Builder<Cell<T>> ret = ImmutableList.builder();
		try (ResultScanner scanner = hTable.hTable.getScanner(scan)) {
			for (Result result = scanner.next(); result != null; result = scanner.next()) {
				ByteString rowKey = ByteString.copyFrom(result.getRow());
				for (Entry<byte[], byte[]> hbaseCell : result.getFamilyMap(family).entrySet()) {
					ret.add(Cell.<T>make(
							rowKey,
							ByteString.copyFrom(hbaseCell.getKey()),
							ByteString.copyFrom(hbaseCell.getValue())));
				}
			}
		}

		return ret.build();
	}

	@Override
	public Iterable<Cell<T>> readColumn(ByteString columnKey) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void close() throws IOException {
		hTable.hTable.close();
	}
}
