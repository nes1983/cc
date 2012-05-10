package ch.unibe.scg.cc.activerecord;

import org.apache.hadoop.hbase.client.Put;

public class FastPutFactory implements IPutFactory {
	public Put create(byte[] rowKey) {
		Put put = new Put(rowKey);
		put.setWriteToWAL(false); // increases performance significally!!
		return put;
	}
}
