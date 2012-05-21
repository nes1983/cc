package ch.unibe.scg.cc.activerecord;

import javax.inject.Named;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.inject.Inject;

public abstract class Column implements Cloneable {
	
	public static final byte[] FAMILY_NAME = Bytes.toBytes("d");
	
	@Inject(optional=true) @Named("strings")
	protected HTable strings;
	
	public Object clone()  {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			throw new AssertionError();
		}
	}
	
}
