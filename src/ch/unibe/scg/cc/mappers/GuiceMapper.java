package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

import ch.unibe.scg.cc.mappers.MRMain.MRMainMapper;

/**
 * Same as a {@link Mapper}, but with the visibility of methods raised to public,
 * to enable hiding a mapper behind a proxy, which is done in {@link MRMainMapper}.
 */
public abstract class GuiceMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}

	@Override
	public void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
		super.map(key, value, context);
	}
}
