package ch.unibe.scg.cells.hadoop;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;

import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.CellSink;

class HBaseCellSink<T> implements CellSink<T> {
	final private HTableWriteBuffer hTable;
	final private PutFactory putFactory;
	final private byte[] family;

	HBaseCellSink(HTableWriteBuffer hTable, PutFactory putFactory, byte[] family) {
		this.hTable = hTable;
		this.putFactory = putFactory;
		this.family = family;
	}

	@Override
	public void close() throws IOException {
		hTable.close();
	}

	@Override
	public void write(Cell<T> cell) {
		Put put = putFactory.create(cell.getRowKey().toByteArray());
		put.add(family, cell.getColumnKey().toByteArray(), cell.getCellContents().toByteArray());
	}
}
