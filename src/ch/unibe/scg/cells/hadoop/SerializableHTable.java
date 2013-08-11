package ch.unibe.scg.cells.hadoop;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.HTable;

import com.google.common.base.Charsets;

class SerializableHTable implements Serializable {
	final private static long serialVersionUID = 1L;

	transient HTable hTable;
	final private HTableFactory tableFactory;

	@Inject
	SerializableHTable(HTable hTable, HTableFactory tableFactory) {
		this.hTable = hTable;
		this.tableFactory = tableFactory;
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
		out.writeUTF(new String(hTable.getTableName(), Charsets.UTF_8));
	}

	private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
		in.defaultReadObject();
		String tableName = in.readUTF();
		hTable = tableFactory.make(tableName);
	}
}
