package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;

public interface IColumn {
	public abstract void save(Put put) throws IOException;
	public abstract byte[] getHash();
}
