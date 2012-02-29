package ch.unibe.scg.cc.activerecord;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class Column implements Cloneable {
	
	protected abstract void save(Put put) ;

	
	public Object clone()  {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			throw new AssertionError();
		}
	}
	
}
