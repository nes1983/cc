package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Map.Entry;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import ch.unibe.scg.cc.activerecord.ConfigurationProvider;
import ch.unibe.scg.cc.mappers.MRMain.MRMainMapper;
import ch.unibe.scg.cc.mappers.MRMain.MRMainReducer;
import ch.unibe.scg.cc.mappers.MRMain.MRMainTableMapper;
import ch.unibe.scg.cc.mappers.MRMain.MRMainTableReducer;

import com.google.common.base.Optional;

public class MRWrapper {
	@Inject
	private ConfigurationProvider configurationProvider;

	@Deprecated
	@SuppressWarnings("rawtypes")
	public Job createMapJob(String jobName, Class<?> jobClassName, String mapperClassName,
			Class<? extends WritableComparable> outputKey, Class<? extends Writable> outputValue) throws IOException,
			InterruptedException, ClassNotFoundException {

		Class<MRMainMapper> mapperClass = MRMainMapper.class;

		Job thisJob = initMapReduceJob(jobName, jobClassName, mapperClass, null, outputKey, outputValue);

		thisJob.getConfiguration().set("GuiceMapperAnnotation", mapperClassName);

		return thisJob;
	}

	@Deprecated
	@SuppressWarnings("rawtypes")
	public boolean launchTableMapReduceJob(String jobName, String tableNameMapper, String tableNameReducer,
			Scan tableScanner, Class<?> jobClassName, String mapperClassName, String reducerClassName,
			Class<? extends WritableComparable> outputKey, Class<? extends Writable> outputValue,
			String mapred_child_java_opts) throws IOException, InterruptedException, ClassNotFoundException {

		Job thisJob;
		Class<MRMainTableMapper> mapperClass = MRMainTableMapper.class;
		Class<MRMainTableReducer> reducerClass = MRMainTableReducer.class;

		thisJob = initMapReduceJob(jobName, jobClassName, mapperClass, reducerClass, outputKey, outputValue);

		TableMapReduceUtil.initTableMapperJob(tableNameMapper, tableScanner, mapperClass, outputKey, outputValue,
				thisJob);
		TableMapReduceUtil.initTableReducerJob(tableNameReducer, reducerClass, thisJob);

		thisJob.getConfiguration().set("GuiceMapperAnnotation", mapperClassName);
		thisJob.getConfiguration().set("GuiceReducerAnnotation", reducerClassName);

		// thisJob.getConfiguration().set("mapreduce.job.maps", "600");

		// thisJob.getConfiguration().set("mapred.child.java.opts",
		// mapred_child_java_opts);

		// for profiling
		thisJob.setProfileEnabled(true);
		thisJob.setProfileParams("-agentlib:hprof=cpu=samples,force=n,thread=y,interval=100,verbose=n,doe=y,file=%s");

		return thisJob.waitForCompletion(true);
	}

	@Deprecated
	@SuppressWarnings("rawtypes")
	private Job initMapReduceJob(String jobName, Class<?> jobClassName, Class<? extends Mapper> mapperClassName,
			Class<? extends Reducer> reducerClassName, Class<?> outputKey, Class<?> outputValue) throws IOException {

		Job thisJob = Job.getInstance(configurationProvider.get(), jobName);

		thisJob.setJarByClass(jobClassName);
		thisJob.setMapperClass(mapperClassName);

		if (reducerClassName != null) {
			thisJob.setReducerClass(reducerClassName);
		}

		thisJob.setMapOutputKeyClass(outputValue);
		thisJob.setMapOutputValueClass(outputValue);

		return thisJob;
	}

	public void truncate(HTable hTable) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(configurationProvider.get());
		HTableDescriptor tableDescription = hTable.getTableDescriptor();
		String tableName = tableDescription.getNameAsString();
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		admin.createTable(tableDescription);
		admin.close();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean launchMapReduceJob(String jobName, Configuration config, Optional<String> mapperTableName,
			Optional<String> reducerTableName, Scan tableScanner, String mapperClassName,
			Optional<String> reducerClassName, Class<? extends WritableComparable> outputKey,
			Class<? extends Writable> outputValue) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration merged = merge(configurationProvider.get(), config);
		Job thisJob = Job.getInstance(merged, jobName);
		thisJob.setJarByClass(MRMain.class);

		Class<?> mapperClass = (mapperTableName.isPresent()) ? MRMainTableMapper.class : MRMainMapper.class;
		Class<?> reducerClass = (reducerTableName.isPresent()) ? MRMainTableReducer.class : MRMainReducer.class;

		// mapper configuration
		if (isSubclassOf(mapperClass, TableMapper.class)) {
			if (!mapperTableName.isPresent()) {
				throw new IllegalArgumentException("mapperTableName argument is not set!");
			}
			TableMapReduceUtil.initTableMapperJob(mapperTableName.get(), tableScanner,
					(Class<? extends TableMapper>) mapperClass, outputKey, outputValue, thisJob);
		} else {
			thisJob.setMapperClass((Class<? extends Mapper>) mapperClass);
		}
		thisJob.setMapOutputKeyClass(outputKey);
		thisJob.setMapOutputValueClass(outputValue);

		// reducer configuration
		if (reducerClassName.isPresent()) {
			if (isSubclassOf(reducerClass, TableReducer.class)) {
				if (!reducerTableName.isPresent()) {
					throw new IllegalArgumentException(
							"tableNameReducer argument is set, but reducerClass is not a subclass of the TableReducer class!");
				}
				TableMapReduceUtil.initTableReducerJob(reducerTableName.get(),
						(Class<? extends TableReducer>) reducerClass, thisJob);
			} else {
				thisJob.setReducerClass((Class<? extends Reducer>) reducerClass);

			}
		}
		if (reducerTableName.isPresent() && !reducerClassName.isPresent()) {
			throw new IllegalArgumentException("If you set the table name, you'll have to give a reducer, too!");
		}

		// guice configuration
		thisJob.getConfiguration().set(GuiceResource.GUICE_MAPPER_ANNOTATION_STRING, mapperClassName);
		if (reducerClassName.isPresent()) {
			thisJob.getConfiguration().set(GuiceResource.GUICE_REDUCER_ANNOTATION_STRING, reducerClassName.get());
		}

		return thisJob.waitForCompletion(true);
	}

	static boolean isSubclassOf(Class<?> subClass, Class<?> superClass) {
		try {
			subClass.asSubclass(superClass);
		} catch (ClassCastException e) {
			return false;
		}
		return true;
	}

	private Configuration merge(Configuration lowPriority, Configuration highPriority) {
		Configuration merged = new Configuration(lowPriority);
		for (Entry<String, String> entry : highPriority) {
			merged.set(entry.getKey(), entry.getValue());
		}
		return merged;
	}
}