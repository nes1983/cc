package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
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
import ch.unibe.scg.cc.mappers.MakeFunction2FineClones.CommonSnippetWritable;

import com.google.common.base.Optional;

/** Mapper and Reducer only used for sorting */
public class Function2FineClonesSorter implements Runnable {
	static final String OUT_DIR = "/tmp/fineclonessorted/";
	final MRWrapper hbaseWrapper;

	@Inject
	Function2FineClonesSorter(MRWrapper hbaseWrapper) {
		this.hbaseWrapper = hbaseWrapper;
	}

	static class IdentityMapper extends
			GuiceMapper<CommonSnippetWritable, NullWritable, CommonSnippetWritable, NullWritable> {
		@Override
		public void map(CommonSnippetWritable key, NullWritable value, Context context) throws IOException,
				InterruptedException {
			context.write(key, value);
		}
	}

	static class IdentityReducer extends
			GuiceReducer<CommonSnippetWritable, NullWritable, CommonSnippetWritable, NullWritable> {
		@Override
		public void reduce(CommonSnippetWritable key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
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

			hbaseWrapper.launchMapReduceJob(Function2FineClonesSorter.class.getName() + " Job", config,
					Optional.<String> absent(), Optional.<String> absent(), Optional.<Scan> absent(),
					IdentityMapper.class.getName(), Optional.of(IdentityReducer.class.getName()),
					CommonSnippetWritable.class, NullWritable.class);
		} catch (IOException | ClassNotFoundException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (InterruptedException e) {
			return; // Exit.
		}
	}
}
