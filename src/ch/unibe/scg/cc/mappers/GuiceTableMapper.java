package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * {@link #write(Put)} provides the ability to write puts to the custom
 * {@link HTableWriteBuffer}.
 * 
 * @see GuiceMapper
 */
public abstract class GuiceTableMapper<KEYOUT, VALUEOUT> extends
		GuiceMapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT> {

	private HTableWriteBuffer writeBuffer;

	public GuiceTableMapper() {
		super();
	}

	public GuiceTableMapper(HTable htable) {
		super();
		this.writeBuffer = new HTableWriteBuffer(htable);
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	// Closes the WriteBuffer when the map-phase is finished
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		if (this.writeBuffer != null) {
			this.writeBuffer.close();
		}
	}

	@Override
	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		super.map(key, value, context);
	}

	protected final void write(Put put) throws IOException {
		if (this.writeBuffer == null) {
			throw new RuntimeException("No HTable was passed to the GuiceTableMapper so don't try to write on it!");
		}
		this.writeBuffer.write(put);
	}
}
