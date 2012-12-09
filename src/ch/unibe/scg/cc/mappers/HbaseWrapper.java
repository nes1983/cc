package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import ch.unibe.scg.cc.activerecord.ConfigurationProvider;
import ch.unibe.scg.cc.mappers.MRMain.MRMainMapper;
import ch.unibe.scg.cc.mappers.MRMain.MRMainTableMapper;
import ch.unibe.scg.cc.mappers.MRMain.MRMainTableReducer;

public class HbaseWrapper {
	@Inject
	private ConfigurationProvider configurationProvider;

	@SuppressWarnings("rawtypes")
	public Job createMapJob(String jobName, 
			Class<?> jobClassName,
			String mapperClassName,
			Class<? extends WritableComparable> outputKey,
			Class<? extends Writable> outputValue) throws IOException, InterruptedException, ClassNotFoundException {

		Class<MRMainMapper> mapperClass = MRMainMapper.class;
		
		Job thisJob = initMapReduceJob(jobName, jobClassName, mapperClass, null, outputKey, outputValue);
		
		thisJob.getConfiguration().set("GuiceMapperAnnotation", mapperClassName);

		return thisJob;
	}

	@SuppressWarnings("rawtypes")
	public boolean launchTableMapReduceJob(String jobName, 
			String tableNameMapper, String tableNameReducer,
			Scan tableScanner, Class<?> jobClassName,
			String mapperClassName,
			String reducerClassName,
			Class<? extends WritableComparable> outputKey,
			Class<? extends Writable> outputValue) throws IOException, InterruptedException, ClassNotFoundException {

		Job thisJob;
		Class<MRMainTableMapper> mapperClass = MRMainTableMapper.class;
		Class<MRMainTableReducer> reducerClass = MRMainTableReducer.class;
		
		thisJob = initMapReduceJob(jobName, jobClassName, mapperClass, reducerClass, outputKey, outputValue);
		
		TableMapReduceUtil.initTableMapperJob(tableNameMapper, tableScanner, mapperClass, outputKey, outputValue, thisJob);
		TableMapReduceUtil.initTableReducerJob(tableNameReducer, reducerClass, thisJob);
		
		thisJob.getConfiguration().set("GuiceMapperAnnotation", mapperClassName);
		thisJob.getConfiguration().set("GuiceReducerAnnotation", reducerClassName);

		return thisJob.waitForCompletion(true);
	}

	@SuppressWarnings("rawtypes")
	private Job initMapReduceJob(
			String jobName,
			Class<?> jobClassName,
			Class<? extends Mapper> mapperClassName,
			Class<? extends Reducer> reducerClassName, 
			Class<?> outputKey,
			Class<?> outputValue) throws IOException {

		Job thisJob = new Job(configurationProvider.get(), jobName);

		thisJob.setJarByClass(jobClassName);
		thisJob.setMapperClass(mapperClassName);
		
		if(reducerClassName != null) {
			thisJob.setReducerClass(reducerClassName);
		}

		thisJob.setMapOutputKeyClass(outputValue);
		thisJob.setMapOutputValueClass(outputValue);

//		thisJob.setInputFormatClass(TableInputFormat.class);// done by TableMapReduceUtil map
//		thisJob.setOutputFormatClass(TableOutputFormat.class); //done by TableMapReduceUtil reduce

		return thisJob;
	}

	public void truncate(HTable hTable) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(configurationProvider.get());
		HTableDescriptor tableDescription = hTable.getTableDescriptor();
		String tableName = tableDescription.getNameAsString();
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		admin.createTable(tableDescription);
	}
}