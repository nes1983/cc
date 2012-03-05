package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public abstract class Column implements Cloneable {
	
	@Inject @Named("strings")
	protected HTable strings;
	
	protected abstract void save(Put put) throws IOException ;

	
	public Object clone()  {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			throw new AssertionError();
		}
	}
	
}
