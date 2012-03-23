package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public abstract class Column implements Cloneable {
	
	public static final String FAMILY_NAME = "d";
	
	@Inject @Named("strings")
	protected HTable strings;
	
	public abstract void save(Put put) throws IOException ;

	public abstract byte[] getHash();
	
	public Object clone()  {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			throw new AssertionError();
		}
	}
	
}
