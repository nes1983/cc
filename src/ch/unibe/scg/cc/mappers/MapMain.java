package ch.unibe.scg.cc.mappers;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapMain {
	  public static void main(String[] args) throws Exception {

		   Configuration conf = new Configuration();
		   GenericOptionsParser p = new GenericOptionsParser(conf, args);
		   System.out.println("hi");
		   System.out.println(Arrays.toString(p.getLibJars(conf))); 
		   Job job = new Job(conf, "mkindex");
		    job.setMapperClass(MakeIndexFactToProject.class);

		        
		    job.waitForCompletion(true);
	  }
}
