package ch.unibe.scg.cells.hadoop;

import java.io.Closeable;
import java.io.IOException;
import java.security.SecureRandom;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import ch.unibe.scg.cells.Annotations.FamilyName;
import ch.unibe.scg.cells.hadoop.Annotations.IndexFamily;

import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;

class TableAdmin {
	final private Configuration configuration;
	final private ByteString family;
	final private ByteString indexFamily;
	final private HTableFactory hTableFactory;

	@Inject
	TableAdmin(Configuration configuration, @FamilyName ByteString family, @IndexFamily ByteString indexFamily,
			HTableFactory hTableFactory) {
		this.configuration = configuration;
		this.family = family;
		this.indexFamily = indexFamily;
		this.hTableFactory = hTableFactory;
	}

	static interface Table<T> extends Closeable {
		byte[] getTableName();
	}

	class TemporaryTable implements Table {
		final HTable table;

		TemporaryTable(HTable table) {
			this.table = table;
		}

		@Override
		public void close() throws IOException {
			table.close();

			byte[] tableName = getTableName();
			try (HBaseAdmin admin = new HBaseAdmin(configuration)) {
				if (admin.isTableAvailable(tableName)) {
					if (admin.isTableEnabled(tableName)) {
						admin.disableTable(tableName);
					}
					admin.deleteTable(tableName);
				}
			}
		}

		@Override
		public byte[] getTableName() {
			return table.getTableName();
		}
	}

	/**
	 * Create a temporary hbase table.
	 * @return a Temporary table, containing an HTable. Don't forget to close it!
	 */
	TemporaryTable createTemporaryTable() throws IOException {
		SecureRandom random = new SecureRandom();
		byte bytes[] = new byte[20];
		random.nextBytes(bytes);
		String tableName = "tmp" + BaseEncoding.base16().encode(bytes);

		try (HBaseAdmin admin = new HBaseAdmin(configuration)) {
			HTableDescriptor td = new HTableDescriptor(tableName);
			td.addFamily(new HColumnDescriptor(family.toByteArray()));
			td.addFamily(new HColumnDescriptor(indexFamily.toByteArray()));

			admin.createTable(td);
		}

		return new TemporaryTable(hTableFactory.make(tableName));
	}
}
