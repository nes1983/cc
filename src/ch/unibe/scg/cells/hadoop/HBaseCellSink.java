package ch.unibe.scg.cells.hadoop;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;

import ch.unibe.scg.cells.Annotations.FamilyName;
import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.CellSink;
import ch.unibe.scg.cells.hadoop.Annotations.WriteToWalEnabled;

import com.google.protobuf.ByteString;

class HBaseCellSink<T> implements CellSink<T> {
	final private HTableWriteBuffer hTable;
	final private byte[] family;
	final private boolean writeToWalEnabled;

	@Inject
	HBaseCellSink(HTableWriteBuffer hTable, @FamilyName ByteString family, @WriteToWalEnabled boolean writeToWalEnabled) {
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
