package ch.unibe.scg.cells.hadoop;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.hadoop.hbase.client.HTable;

import ch.unibe.scg.cells.hadoop.HBaseStorage.TableName;


/** Assumes that CellsModule is also installed. Sets default values on the table. */
class HTableProvider implements Provider<HTable> {
	final private HTableFactory factory;
	final private String tableName;

	@Inject
	HTableProvider(HTableFactory factory, @TableName String tableName) {
		this.tableName = tableName;
		this.factory = factory;
	}

	@Override
	public HTable get() {
		return factory.make(tableName);
	}
}
