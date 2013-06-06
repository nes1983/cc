package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import ch.unibe.scg.cc.mappers.MakeFunction2FineClones.CommonSnippetWritable;

/** Mapper and Reducer only used for sorting */
public class Function2FineClonesSorter {
	static final Path OUT_DIR = new Path("/tmp/fineclonessorted/");

	public static class IdentityMapper extends
			Mapper<CommonSnippetWritable, NullWritable, CommonSnippetWritable, NullWritable> {
		@Override
		public void map(CommonSnippetWritable key, NullWritable value, Context context) throws IOException,
				InterruptedException {
			context.write(key, value);
		}
	}

	public static class IdentityReducer extends
			Reducer<CommonSnippetWritable, NullWritable, CommonSnippetWritable, NullWritable> {
		@Override
		public void reduce(CommonSnippetWritable key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			for (NullWritable value : values) {
				context.write(key, value);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "sort the clones");

		job.setOutputKeyClass(CommonSnippetWritable.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(IdentityMapper.class);
		job.setReducerClass(IdentityReducer.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPaths(job, "hdfs://haddock/" + MakeFunction2FineClones.OUT_DIR);
		FileOutputFormat.setOutputPath(job, OUT_DIR);

		job.waitForCompletion(true);
	}

}
