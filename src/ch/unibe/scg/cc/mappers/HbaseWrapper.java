package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
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

	@SuppressWarnings("rawtypes")
	public static boolean launchMapReduceJob(String jobName, 
			String tableNameMapper, String tableNameReducer,
			Scan tableScanner, Class<?> jobClassName,
			Class<? extends TableMapper> mapperClassName,
			Class<? extends TableReducer> reducerClassName,
			Class<? extends WritableComparable> outputKey,
			Class<? extends Writable> outputValue) {

		Job thisJob;
		try {
			thisJob = initMapReduceJob(jobName, tableScanner,
					jobClassName, mapperClassName, reducerClassName, outputKey,
					outputValue);

			TableMapReduceUtil.initTableMapperJob(tableNameMapper, tableScanner,
					mapperClassName, outputKey, outputValue, thisJob);
			TableMapReduceUtil.initTableReducerJob(tableNameReducer, reducerClassName,
					thisJob);
			
			// experimental
//			thisJob.setOutputFormatClass(HFileOutputFormat.class);
//			HTable table = new HTable(tableNameReducer);
//			table.setAutoFlush(false);
//			HFileOutputFormat.configureIncrementalLoad(thisJob, table);

			return thisJob.waitForCompletion(true);

		} catch (Exception e) {
			throw new RuntimeException(e.toString());
		}
	}

	@SuppressWarnings("rawtypes")
	public static Job initMapReduceJob(String jobName,
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

//		thisJob.setInputFormatClass(TableInputFormat.class);// done by TableMapReduceUtil map
//		thisJob.setOutputFormatClass(TableOutputFormat.class); //done by TableMapReduceUtil reduce

		return thisJob;
	}

	private static Configuration getConfiguration() {
		if (configuration == null) {
			configuration = HBaseConfiguration.create();
			//configuration.set("mapred.job.tracker", "pinocchio:50030");
		}
		return configuration;
	}

	public static void truncate(HTable hTable) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(getConfiguration());
		HTableDescriptor tableDescription = hTable.getTableDescriptor();
		String tableName = tableDescription.getNameAsString();
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		admin.createTable(tableDescription);
	}
}