package ch.unibe.scg.cells.benchmarks;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import com.google.common.base.Charsets;
import com.google.inject.Guice;

import ch.unibe.scg.cells.benchmarks.SVM;
import ch.unibe.scg.cells.benchmarks.SVM.TrainingInstance;
import ch.unibe.scg.cells.hadoop.UnibeModule;

/**
 * A plain hadoop job for training a distributed svm. Map phase distributes data across machines,
 * reduce phase trains a set of svms on an subset of data.
 */
public class HadoopSVMBenchmark {
	final static String TEST_TABLE = HadoopSVMBenchmark.class.getSimpleName();
	final static String INPUT_PATH = "hdfs://haddock.unibe.ch/tmp/svmdata";
	final static int DEFAULT_SHARDS = 80;

	/**
	 * The Map class has to make sure that the data is shuffled to the various machines.
	 */
	public static class Map extends Mapper<ImmutableBytesWritable, ImmutableBytesWritable, LongWritable, Text> {
		final private  Text outValue = new Text();
		final private  LongWritable outKey = new LongWritable();

		private int shards;

		/**
		 * Spread the data around on K different machines.
		 */
		@Override
		public void map(ImmutableBytesWritable key, ImmutableBytesWritable value, Context context) throws IOException, InterruptedException {
			try (Scanner sc = new Scanner(new String(value.get(), Charsets.ISO_8859_1))) {
				int i = 0;
				while (sc.hasNextLine()) {
					if (i == shards) {
						i = 0;
					}

					outValue.set(new String(sc.nextLine()));
					outKey.set(i);
					context.write(outKey, outValue);

					i++;
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException {
			String shardsFromConfig = context.getConfiguration().get("shard.count");
			if (shardsFromConfig == null) {
				shards = DEFAULT_SHARDS;
			} else {
				shards = Integer.parseInt(shardsFromConfig);
			}
		}
	}

	/**
	 * Each of K reducers has to output one file containing the hyperplane.
	 */
	public static class Reduce  extends Reducer<LongWritable, Text, NullWritable, NullWritable> {
		static byte[] FAMILY = "f".getBytes(Charsets.UTF_8);
		static byte[] COLUMN = "c".getBytes(Charsets.UTF_8);
		private HTable htable;

		/**
		 * Construct a hyperplane given the subset of training examples.
		 */
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<TrainingInstance> trainingSet = new LinkedList<>();

			for (Text t: values) {
				String s = t.toString();
				TrainingInstance instance = new TrainingInstance(s);
				trainingSet.add(instance);
			}

			SVM model = SVM.trainSVM(trainingSet, 10000);


			Put put = new Put(key.toString().getBytes(Charsets.UTF_8));
			put.add(FAMILY, COLUMN, model.toString().getBytes(Charsets.UTF_8));
			htable.put(put);
		}

		@Override
		protected void setup(Context context) throws IOException {
			Configuration config = Guice.createInjector(new UnibeModule()).getInstance(Configuration.class);
			htable = new HTable(config, TEST_TABLE);
		}

		@Override
		protected void cleanup(Context context) throws IOException {
			htable.close();
		}
	}

	/**
	 * Runs distributed SVM training job. Takes a hdfs path as parameter.
	 * The default path is "hdfs://haddock.unibe.ch/tmp/svmdata"
	 */
	public static void main(String[] args) throws Exception {
		int shards = DEFAULT_SHARDS;

		String input = INPUT_PATH;

		if (args.length >  0) {
			input = args[0];
		}

		if (args.length > 1) {
			shards = Integer.parseInt(args[2]);
		} else {
			System.out.println("INFO: shard  number was not specified, using the default [" + shards + "]");
		}

		Configuration conf = new Configuration();
		conf.setLong(MRJobConfig.MAP_MEMORY_MB, 2000L);
		conf.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx2200m");

		Job job = Job.getInstance(conf, "Distributed SVM Training");
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(RawFileFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);

		conf.set("shard.count", Integer.toString(shards));
		job.setNumReduceTasks(shards);

		FileInputFormat.addInputPath(job, new Path(input));

		job.setJarByClass(HadoopSVMBenchmark.class);
		job.waitForCompletion(true);
	}
}
