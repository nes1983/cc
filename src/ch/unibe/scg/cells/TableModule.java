package ch.unibe.scg.cells;

import ch.unibe.scg.cells.Annotations.FamilyName;
import ch.unibe.scg.cells.Annotations.TableName;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.protobuf.ByteString;

/**
 * A module that holds all that the table-specific parameters needed to obtain tables.
 * HBase tables, for example, need their table name, and column family set.
 * If that's all you want to set, use {@link DefaultTableModule}.
 *
 * <p>
 * Other backends might need additional parameters. You can set all of them in one module.
 */
public interface TableModule extends Module {
	/**
	 * The table module that contains enough data to initialize both in memory tables and HBase tables.
	 * If you need more backends, define your own TableModule.
	 * Note that from the point of view of this framework, every column family in an
	 * HBase table defines a separate Cells table.
	 */
	public static class DefaultTableModule extends AbstractModule implements TableModule {
		final private String tableName;
		final private ByteString family;

		/** family is the column family of the table to be initialized. */
		public DefaultTableModule(String tableName, ByteString columnFamily) {
			this.tableName = tableName;
			this.family = columnFamily;
		}

		@Override
		protected void configure() {
			bindConstant().annotatedWith(TableName.class).to(tableName);
			bind(ByteString.class).annotatedWith(FamilyName.class).toInstance(family);
		}
	}
}