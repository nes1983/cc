package ch.unibe.scg.cc.activerecord;

import org.apache.hadoop.hbase.client.Put;

public interface IPutFactory {
	Put create(byte[] rowKey);
}
