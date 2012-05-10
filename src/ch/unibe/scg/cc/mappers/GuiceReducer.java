package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;

public abstract class GuiceReducer {
	
	@Inject
	GuiceResource resource;

	@SuppressWarnings("rawtypes")
	abstract void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException;

}
