package ch.unibe.scg.cc.mappers;

import java.net.InetSocketAddress;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import ch.unibe.scg.cc.mappers.WordCountTest.Mapper1;
import ch.unibe.scg.cc.mappers.WordCountTest.Reducer1;

public class MapMain {
	public static void main(String[] args) throws Exception {

//		Configuration conf = HBaseConfiguration.create();
//		conf.set("mapred.job.tracker", "pinocchio:50030");

		//GenericOptionsParser p = new GenericOptionsParser(conf, args);
		//System.out.println(Arrays.toString(p.getLibJars(conf)));

		System.out.println("hi");
		
		
		Scan scan = new Scan();
		HbaseWrapper.launchMapReduceJob(
		                        "WordCountTestJob", "projects", scan, 
		                        WordCountTest.class, Mapper1.class, 
		                        Reducer1.class, Text.class, IntWritable.class);
		
//		JobConf jobConf = new JobConf(conf);
//		jobConf.setJarByClass(MapMain.class);
//
//		InetSocketAddress isa = new InetSocketAddress("pinocchio", 50030);
//		JobClient jobClient = new JobClient(isa, conf);
//		jobClient.submitJob(jobConf);

		// TableMapReduceUtil.initTableMapperJob("functions", new Scan(),
		// MakeIndexFactToProject.class, ImmutableBytesWritable.class,
		// BytesWritable.class, job);

		// job.setJarByClass(MapMain.class);
		// job.setOutputFormatClass(NullOutputFormat.class);
		// job.setOutputFormatClass(NullOutputFormat.class);
		// job.setMapperClass(MakeIndexFactToProject.class);
		// System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
