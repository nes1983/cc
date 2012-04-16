package ch.unibe.scg.cc.mappers;

import java.net.InetSocketAddress;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapMain {
	  public static void main(String[] args) throws Exception {

		   Configuration conf = HBaseConfiguration.create();
		   GenericOptionsParser p = new GenericOptionsParser(conf, args);
		   System.out.println("hi");
		   System.out.println(Arrays.toString(p.getLibJars(conf))); 
		   Job job = new Job(conf, "mkindex");
		   job.setJarByClass(MapMain.class);
		   
		   
		   job.setOutputFormatClass(NullOutputFormat.class);
		   
		   TableMapReduceUtil.initTableMapperJob("functions", new Scan(), MakeIndexFactToProject.class, ImmutableBytesWritable.class, BytesWritable.class, job);
		   
		   
//		   InetSocketAddress isa = new InetSocketAddress("", 1234);
//		   JobClient jc = new JobClient(jobTrackAddr, conf);
		   
//		   job.setOutputFormatClass(NullOutputFormat.class);
//		   job.setMapperClass(MakeIndexFactToProject.class);

		        
		   System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
