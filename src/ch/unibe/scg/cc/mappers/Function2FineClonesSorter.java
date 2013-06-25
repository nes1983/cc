package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import ch.unibe.scg.cc.WrappedRuntimeException;

import com.google.common.base.Optional;

/** Mapper and Reducer only used for sorting */
public class Function2FineClonesSorter implements Runnable {
	static final String OUT_DIR = "/tmp/fineclonessorted/";
	final MRWrapper mrWrapper;

	@Inject
	Function2FineClonesSorter(MRWrapper mrWrapper) {
		this.mrWrapper = mrWrapper;
	}

	static class IdentityMapper extends GuiceMapper<BytesWritable, NullWritable, BytesWritable, NullWritable> {
		@Override
		public void map(BytesWritable key, NullWritable value, Context context) throws IOException,
				InterruptedException {
			context.write(key, value);
		}
	}

	static class IdentityReducer extends GuiceReducer<BytesWritable, NullWritable, BytesWritable, NullWritable> {
		@Override
		public void reduce(BytesWritable key, Iterable<NullWritable> values, Context context) throws IOException,
				InterruptedException {
			for (NullWritable value : values) {
				context.write(key, value);
			}
		}
	}

	@Override
	public void run() {
		try {
			FileSystem.get(new Configuration()).delete(new Path(OUT_DIR), true);

			Configuration config = new Configuration();
			config.set(MRJobConfig.MAP_MEMORY_MB, "1536");
			config.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx1024M");
			config.set(MRJobConfig.REDUCE_MEMORY_MB, "3072");
			config.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx2560M");
			config.set(FileInputFormat.INPUT_DIR, "hdfs://haddock/" + MakeFunction2FineClones.OUT_DIR);
			config.set(FileOutputFormat.OUTDIR, OUT_DIR);
			config.setClass(Job.INPUT_FORMAT_CLASS_ATTR, SequenceFileInputFormat.class, InputFormat.class);
			config.setClass(Job.OUTPUT_FORMAT_CLASS_ATTR, TextOutputFormat.class, OutputFormat.class);

			mrWrapper.launchMapReduceJob(Function2FineClonesSorter.class.getName() + " Job", config,
					Optional.<String> absent(), Optional.<String> absent(), Optional.<Scan> absent(),
					IdentityMapper.class.getName(), Optional.of(IdentityReducer.class.getName()), BytesWritable.class,
					NullWritable.class);
		} catch (IOException | ClassNotFoundException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return; // Exit.
		}
	}
}
