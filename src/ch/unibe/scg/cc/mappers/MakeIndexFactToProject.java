package ch.unibe.scg.cc.mappers;

import java.io.File;
import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import   org.apache.hadoop.util.GenericOptionsParser;



public class MakeIndexFactToProject extends TableMapper<Text, Text> {

	
	
	final HTable functions;
	
	@Inject
	public MakeIndexFactToProject(@Named("functions") HTable functions) {
		this.functions = functions;
	}
	
	public void map(ImmutableBytesWritable row, Result value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		if(true) 
			throw new RuntimeException("waa");
			
	      
	   }
	
}
