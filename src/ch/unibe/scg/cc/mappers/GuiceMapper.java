package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;

public abstract class GuiceMapper {
	
	@Inject
	GuiceResource resource;

	@SuppressWarnings("rawtypes")
	abstract void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException;

}
