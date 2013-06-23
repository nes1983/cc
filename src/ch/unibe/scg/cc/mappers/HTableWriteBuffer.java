package ch.unibe.scg.cc.mappers;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import ch.unibe.scg.cc.activerecord.HTableProvider;

import com.google.inject.assistedinject.Assisted;

/**
 * Collects the puts in the buffer and writes them to HBase as soon as
 * {@link #MAX_SIZE} is reached. Ensure to call {@link #close()} after the last
 * Put got passed to the Buffer.
 */
public class HTableWriteBuffer implements Closeable {
	private static final int MAX_SIZE = 10000;
	final List<Row> puts;
	final HTable htable;

	@Inject
	HTableWriteBuffer(@Assisted HTable htable) {
		this.htable = htable;
		this.puts = new ArrayList<Row>(MAX_SIZE);
	}

	public void write(Put put) throws IOException {
		assert put != null;
		puts.add(put);
		if (puts.size() == MAX_SIZE) {
			HTableUtil.bucketRsBatch(htable, puts);
			puts.clear();
		}
		assert invariant();
	}

	/** HTableWriteBuffer can be produced both by injection and by factory */
	// TODO: Why are there two ways?! There should be just one!
	static class HTableWriteBufferProvider implements Provider<HTableWriteBuffer> {
		@Inject
		HTableProvider htableProvider;

		@Inject
		BufferFactory bufferFactory;

		@Override
		public HTableWriteBuffer get() {
			return bufferFactory.create(htableProvider.get());
		}
	}

	private void writeRemainingPuts() throws IOException {
		HTableUtil.bucketRsBatch(htable, puts);
		assert invariant();
	}

	protected boolean invariant() {
		return puts.size() <= MAX_SIZE;
	}

	/** Factory thru assisted inject for {@link HTableWriteBuffer} */
	public static interface BufferFactory {
		public HTableWriteBuffer create(HTable htable);
	}

	/** Writes the remaining puts. */
	@Override
	public void close() throws IOException {
		writeRemainingPuts();
	}
}