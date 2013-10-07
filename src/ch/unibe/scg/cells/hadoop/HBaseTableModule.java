package ch.unibe.scg.cells.hadoop;

import ch.unibe.scg.cells.TableModule;
import ch.unibe.scg.cells.hadoop.HBaseStorage.FamilyName;
import ch.unibe.scg.cells.hadoop.HBaseStorage.TableName;

import com.google.inject.AbstractModule;
import com.google.protobuf.ByteString;

/**
 * The table module that contains enough data to initialize and HBase tables.
 * If you need more backends, define your own TableModule.
 * From the point of view of this framework, every column family in an
 * HBase table defines a separate Cells table.
 *
 * <p>
 * Note that, because InMemory tables don't need any table information, this module alone
 * can initialize both inmemory tables, and HBase modules.
 *
 * @param <T> The type of raw data stored in the table.
 */
public final class HBaseTableModule<T> extends AbstractModule implements TableModule {
	final private String tableName;
	final private ByteString family;

	/** Note that, from Cell's point of view, different column families mean different tables. */
	public HBaseTableModule(String tableName, ByteString columnFamily) {
		// TODO: Is this constructor really needed?
		this.tableName = tableName;
		this.family = columnFamily;
	}

	/** Extracts name and column family from tab. */
	public HBaseTableModule(Table<T> tab) {
		this(tab.getTableName(), tab.getFamilyName());
	}

	@Override
	protected void configure() {
		bindConstant().annotatedWith(TableName.class).to(tableName);
		bind(ByteString.class).annotatedWith(FamilyName.class).toInstance(family);
	}
}

