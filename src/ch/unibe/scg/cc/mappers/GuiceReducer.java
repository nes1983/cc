package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public abstract class GuiceReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	public GuiceReducer() {
		super();
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}

	@Override
	protected void reduce(KEYIN key, Iterable<VALUEIN> values, Context context) throws IOException,
			InterruptedException {
		super.reduce(key, values, context);
	}
}
