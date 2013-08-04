package ch.unibe.scg.cells.hadoop;

import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

import ch.unibe.scg.cells.Annotations.TableName;

/** Assumes that CellsModule is also installed. Sets default values on the table. */
class HTableProvider implements Provider<HTable> {
	final private Configuration hbaseConfig;
	final private String tableName;

	@Inject
	HTableProvider(Configuration hbaseConfig, @TableName String tableName) {
		this.hbaseConfig = hbaseConfig;
		this.tableName = tableName;
	}

	@Override
	public HTable get() {
		HTable htable;
		try {
			htable = new HTable(hbaseConfig, tableName);
			htable.setAutoFlush(false);
			htable.setWriteBufferSize(1024 * 1024 * 12);
			htable.getTableDescriptor().setDeferredLogFlush(true);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return htable;
	}
}
