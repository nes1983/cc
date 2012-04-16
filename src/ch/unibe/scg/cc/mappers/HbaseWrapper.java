package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class HbaseWrapper {
	private static Configuration configuration = null;

	public static boolean launchMapReduceJob(String jobName, String tableName,
			Scan tableScanner, Class<?> jobClassName,
			Class<? extends TableMapper> mapperClassName,
			Class<? extends TableReducer> reducerClassName,
			Class<? extends WritableComparable> outputKey,
			Class<? extends Writable> outputValue) {

		Job thisJob;
		try {
			thisJob = initMapReduceJob(jobName, tableName, tableScanner,
					jobClassName, mapperClassName, reducerClassName, outputKey,
					outputValue);

			TableMapReduceUtil.initTableMapperJob(tableName, tableScanner,
					mapperClassName, outputKey, outputValue, thisJob);
			TableMapReduceUtil.initTableReducerJob(tableName, reducerClassName,
					thisJob);

			return thisJob.waitForCompletion(true);

		} catch (Exception e) {
			throw new RuntimeException(e.toString());
		}
	}

	public static Job initMapReduceJob(String jobName, String tableName,
			Scan tableScanner, Class<?> jobClassName,
			Class<? extends Mapper> mapperClassName,
			Class<? extends Reducer> reducerClassName, Class<?> outputKey,
			Class<?> outputValue) throws IOException {

		Job thisJob = new Job(getConfiguration(), jobName);

		thisJob.setJarByClass(jobClassName);
		thisJob.setNumReduceTasks(3);
		thisJob.setMapperClass(mapperClassName);
		thisJob.setReducerClass(reducerClassName);

		thisJob.setMapOutputKeyClass(outputValue);
		thisJob.setMapOutputValueClass(outputValue);

		thisJob.setInputFormatClass(TableInputFormat.class);
		thisJob.setOutputFormatClass(TableOutputFormat.class);

		return thisJob;
	}

	private static Configuration getConfiguration() {
		if (configuration == null) {
			configuration = HBaseConfiguration.create();
			//configuration.set("mapred.job.tracker", "pinocchio:50030");
		}
		return configuration;
	}
}