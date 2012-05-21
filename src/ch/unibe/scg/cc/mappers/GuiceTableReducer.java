package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

public abstract class GuiceTableReducer<KEYIN, VALUEIN, KEYOUT> extends TableReducer<KEYIN, VALUEIN, KEYOUT> {
	
	final HTableWriteBuffer writeBuffer;

	public GuiceTableReducer(HTable htable) {
		super();
		this.writeBuffer = new HTableWriteBuffer(htable);
	}
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		this.writeBuffer.writeRemainingPuts();
	}
	
	@Override
	public void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException, InterruptedException {
		super.reduce(key, values, context);
	}
	
	/**
	 * 
	 * @param put not null.
	 * @throws IOException
	 */
	protected final void write(Put put) throws IOException {
		this.writeBuffer.write(put);
	}
}
