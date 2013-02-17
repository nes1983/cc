package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import ch.unibe.scg.cc.mappers.MRMain.MRMainReducer;

/**
 * Acts as a proxy for {@link Reducer}. Except the run method is not visible
 * because the {@link MRMainReducer} explicitly calls the run method of its
 * superclass {@link Reducer}.
 */
public abstract class GuiceReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

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
