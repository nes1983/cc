package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public abstract class GuiceTableMapper<KEYOUT, VALUEOUT> extends
		GuiceMapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT> {

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
	}

	@Override
	public void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		super.map(key, value, context);
	}
}
