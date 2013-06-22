package ch.unibe.scg.cc.activerecord;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.hadoop.hbase.client.Put;

public class PutFactory {
	final boolean writeToWalEnabled;

	@Inject
	PutFactory(@Named("writeToWalEnabled") boolean writeToWalEnabled) {
		this.writeToWalEnabled = writeToWalEnabled;
	}

	public Put create(byte[] rowKey) {
		Put put = new Put(rowKey);
		put.setWriteToWAL(writeToWalEnabled);
		return put;
	}
}
