package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import ch.unibe.scg.cc.mappers.inputformats.ZipFileInputFormat;

public class TablePopulator {
	public static class Map extends
			Mapper<Text, BytesWritable, Text, IntWritable> {
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			System.out.println("blaaaaa: " + key);
			throw new RuntimeException();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();

		Job job = new Job(conf, "populate");
		job.setJarByClass(TablePopulator.class);

//		job.setOutputKeyClass();
//		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
//		job.setReducerClass(null);

		job.setInputFormatClass(ZipFileInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("/project-clone-detector/projects"));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
