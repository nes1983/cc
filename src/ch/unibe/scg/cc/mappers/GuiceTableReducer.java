package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

/**
 * {@link #write(Put)} provides the ability to write puts to the custom
 * {@link HTableWriteBuffer}.
 * 
 * @see GuiceReducer
 */
public abstract class GuiceTableReducer<KEYIN, VALUEIN, KEYOUT> extends TableReducer<KEYIN, VALUEIN, KEYOUT> {

	final HTableWriteBuffer writeBuffer;

	public GuiceTableReducer(HTable htable) {
		super();
		this.writeBuffer = new HTableWriteBuffer(htable);
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	// Closes the WriteBuffer when the reduce-phase is finished
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		this.writeBuffer.close();
	}

	@Override
	protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException,
			InterruptedException {
		super.reduce(key, values, context);
	}

	protected final void write(Put put) throws IOException {
		this.writeBuffer.write(put);
	}
}
