package ch.unibe.scg.cells.hadoop;

import java.io.IOException;
import java.security.SecureRandom;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;

/** Grants access to HBase's tables. */
public class TableAdmin {
	final private Configuration configuration;
	final private HTableFactory hTableFactory;

	@Inject
	TableAdmin(Configuration configuration, HTableFactory hTableFactory) {
		this.configuration = configuration;
		this.hTableFactory = hTableFactory;
	}

	void deleteTable(String tableName) throws IOException {
		try (HBaseAdmin admin = new HBaseAdmin(configuration)) {
			if (admin.isTableAvailable(tableName)) {
				if (admin.isTableEnabled(tableName)) {
					admin.disableTable(tableName);
				}
				admin.deleteTable(tableName);
			}
		}
	}

	/** Create a temporary hbase table. On close, The temporary table gets deleted from HBase. */
	@SuppressWarnings("resource") // Tab gets closed properly in close of return value.
	<T> Table<T> createTemporaryTable(final ByteString family) throws IOException {
		SecureRandom random = new SecureRandom();
		byte bytes[] = new byte[20];
		random.nextBytes(bytes);
		final String tableName = "tmp" + BaseEncoding.base16().encode(bytes);

		final HTable tab = createHTable(tableName, family);
		String decoded = new String(tab.getTableName(), Charsets.UTF_8);
		if (!decoded.equals(tableName)) {
			throw new AssertionError(
					"There are encoding issues with the name we picked. It's coming back as " + decoded);
		}

		return new Table<T>() {
			@Override public void close() throws IOException {
				tab.close();
				deleteTable(getTableName());
			}

			@Override
			public String getTableName() {
				return tableName;
			}

			@Override
			public ByteString getFamilyName() {
				return family;
			}
		};
	}

	/** Create a non-temporary table. On close, the table will NOT be deleted. */
	@SuppressWarnings("resource") // Tab will be properly closed in the return value's close.
	<T> Table<T> createTable(final String tableName, final ByteString family) throws IOException {
		final HTable tab = createHTable(tableName, family);
		String decoded = new String(tab.getTableName(), Charsets.UTF_8);
		if (!decoded.equals(tableName)) {
			throw new IllegalStateException(
					"There are encoding issues with the name you chose. It's coming back as " + decoded);
		}

		return new Table<T>() {
			@Override public void close() throws IOException {
				tab.close();
			}

			@Override public String getTableName() {
				return tableName;
			}

			@Override public ByteString getFamilyName() {
				return family;
			}
		};
	}

	private HTable createHTable(String tableName, ByteString family) throws IOException {
		try (HBaseAdmin admin = new HBaseAdmin(configuration)) {
			HTableDescriptor td = new HTableDescriptor(tableName);
			td.addFamily(new HColumnDescriptor(family.toByteArray()).setCompressionType(Algorithm.SNAPPY));
			td.addFamily(new HColumnDescriptor(HBaseStorage.INDEX_FAMILY.toByteArray()));

			admin.createTable(td);
		}

		return hTableFactory.make(tableName);
	}

	<T> Table<T> existing(final String tabName, final ByteString famName) {
		// TODO: Check if table exists.
		return new Table<T>() {
			@Override public void close() throws IOException {
				// Nothing to do.
			}

			@Override
			public String getTableName() {
				return tabName;
			}

			@Override
			public ByteString getFamilyName() {
				return famName;
			}
		};
	}
}
