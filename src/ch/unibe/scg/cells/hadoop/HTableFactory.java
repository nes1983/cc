package ch.unibe.scg.cells.hadoop;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

/** Make a new HTable object (not the underlying database) for a given name. */
class HTableFactory {
	final private Configuration hbaseConfig;

	@Inject
	HTableFactory(Configuration hbaseConfig) {
		this.hbaseConfig = hbaseConfig;
	}

	public HTable make(String tableName) {
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
