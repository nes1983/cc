package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableUtil;
import org.apache.hadoop.hbase.client.Put;

import com.google.inject.assistedinject.Assisted;

public class HTableWriteBuffer {
	private static final int MAX_SIZE = 10000;
	final List<Put> puts;
	final HTable htable;

	@Inject
	HTableWriteBuffer(@Assisted HTable htable) {
		this.htable = htable;
		this.puts = new ArrayList<Put>(MAX_SIZE);
	}
	
	public void write(Put put) throws IOException {
		assert put != null;
		puts.add(put);
		if(puts.size() == MAX_SIZE ) {
			HTableUtil.bucketRsPut(htable, puts);
			puts.clear();
		}
		assert invariant();
	}

	public void writeRemainingPuts() throws IOException {
		HTableUtil.bucketRsPut(htable, puts);
		assert invariant();
	}
	
	protected boolean invariant() {
		return puts.size() <= MAX_SIZE;
	}
	
	public static interface BufferFactory {
		public HTableWriteBuffer create(HTable htable);
	}
}