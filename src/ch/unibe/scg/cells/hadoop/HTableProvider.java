package ch.unibe.scg.cells.hadoop;

import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

import ch.unibe.scg.cells.hadoop.Annotations.TableName;

class HTableProvider implements Provider<HTable> {
	final private Configuration hbaseConfig;

	@Inject
	HTableProvider(Configuration hbaseConfig, @TableName String tableName) {
		this.hbaseConfig = hbaseConfig;
		this.tableName = tableName;
	}

	final private String tableName;

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
