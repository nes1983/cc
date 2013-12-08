package ch.unibe.scg.cells.benchmarks;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import ch.unibe.scg.cells.hadoop.UnibeModule;

import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;
import com.google.inject.Guice;

/** Count words, reading from HDFS, writing to HBase. */
public class HadoopBenchmark {
	final static String TEST_TABLE = HadoopBenchmark.class.getSimpleName();
	// TODO: Saner path handling.
	final static String INPUT_PATH = "hdfs://haddock.unibe.ch/tmp/books";

	/** Count words in file. */
	public static class WordMapper extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, Text, IntWritable> {
		@Override
		public void map(ImmutableBytesWritable key, ImmutableBytesWritable value, Context context)
				throws IOException, InterruptedException {
			String input = new String(value.get(), Charsets.ISO_8859_1);
			Map<String, Integer> counts = new HashMap<>();

			for (String word : input.split("\\s+")) {
				if (word.isEmpty()) {
					continue;
				}
				if (!counts.containsKey(word)) {
					counts.put(word, 0);
				}
				counts.put(word, counts.get(word) + 1);
			}

			for (Entry<String, Integer> e : counts.entrySet()) {
				context.write(new Text(e.getKey()), new IntWritable(e.getValue()));
			}
		}
	}

	/** Add up counts. */
	public static class WordReduce extends Reducer<Text, IntWritable, NullWritable, NullWritable> {
		static byte[] FAMILY = "f".getBytes(Charsets.UTF_8);
		static byte[] COLUMN = "c".getBytes(Charsets.UTF_8);
		private HTable htable;

		@Override
		protected void setup(Context context) throws IOException {
			Configuration config = Guice.createInjector(new UnibeModule()).getInstance(Configuration.class);
			htable = new HTable(config, TEST_TABLE);
			htable.setAutoFlush(false);
			htable.setWriteBufferSize(1024 * 1024 * 12);
			htable.getTableDescriptor().setDeferredLogFlush(true);
		}

		@Override
		protected void cleanup(Context context) throws IOException {
			htable.close();
		}

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException,	InterruptedException {
			long sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			Put put = new Put(key.toString().getBytes(Charsets.UTF_8));
			put.add(FAMILY, COLUMN, Longs.toByteArray(sum));
			htable.put(put);
		}
	}

	/** Takes no arguments. */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt(MRJobConfig.NUM_REDUCES, 2);

		Job job = Job.getInstance(conf, "hadoop-wordcount-bench");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReduce.class);

		job.setInputFormatClass(RawFileFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));

		job.setJarByClass(HadoopBenchmark.class);
		job.waitForCompletion(true);
	}
}
