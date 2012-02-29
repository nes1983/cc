package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import com.google.inject.throwingproviders.CheckedProvider;

public class HTableProvider implements CheckedProvider<HTable> {

	@Inject @Named("tableName")
	String tableName;
	
	@Override
	public HTable get() throws IOException {
		Configuration hbaseConfig = HBaseConfiguration.create();
        HTable htable;
		htable = new HTable(hbaseConfig, tableName);
        htable.setAutoFlush(false);
        htable.setWriteBufferSize(1024 * 1024 * 12);
		return htable;
	}

}
