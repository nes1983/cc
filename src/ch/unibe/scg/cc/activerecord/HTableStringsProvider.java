package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

public class HTableStringsProvider implements Provider<HTable> {
	
	@Inject
	Configuration hbaseConfig;

	String tableName = "strings";
	
	@Override
	public HTable get() {
        HTable htable;
		try {
			htable = new HTable(hbaseConfig, tableName);
			htable.setAutoFlush(false);
			htable.setWriteBufferSize(1024 * 1024 * 12);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return htable;
	}

}
