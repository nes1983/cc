package ch.unibe.scg.cells.hadoop;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;

import ch.unibe.scg.cells.hadoop.Annotations.WriteToWalEnabled;

class PutFactory {
	final boolean writeToWalEnabled;

	@Inject
	PutFactory(@WriteToWalEnabled boolean writeToWalEnabled) {
		this.writeToWalEnabled = writeToWalEnabled;
	}

	public Put create(byte[] rowKey) {
		Put put = new Put(rowKey);
		put.setWriteToWAL(writeToWalEnabled);
		return put;
	}
}