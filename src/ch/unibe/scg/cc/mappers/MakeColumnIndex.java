package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import ch.unibe.scg.cc.WrappedRuntimeException;
import ch.unibe.scg.cc.activerecord.PutFactory;

import com.google.common.base.Optional;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.name.Names;

class MakeColumnIndex {
	/** inverts mapping: sets the column key as row key and vice versa */
	static class MakeColumnIndexMapper extends GuiceTableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		private static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};
		final PutFactory putFactory;

		@Inject
		MakeColumnIndexMapper(HTableWriteBuffer table, PutFactory putFactory) {
			super(table);
			this.putFactory = putFactory;
		}

		@Override
		public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
				InterruptedException {
			NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(Constants.FAMILY);
			for (Entry<byte[], byte[]> column : familyMap.entrySet()) {
				Put put = putFactory.create(column.getKey());
				put.add(Constants.INDEX_FAMILY, key.get(), 0l, EMPTY_BYTE_ARRAY);
				write(put);
			}
		}
	}

	static class Version2Project implements Runnable {
		public static final String TABLE_NAME = "project2version";
		final MRWrapper mrWrapper;
		final Provider<Scan> scanProvider;

		@Inject
		Version2Project(MRWrapper mrWrapper, Provider<Scan> scanProvider) {
			this.mrWrapper = mrWrapper;
			this.scanProvider = scanProvider;
		}

		@Override
		public void run() {
			launchMapReduceJob(mrWrapper, scanProvider, Version2ProjectModule.class, TABLE_NAME);
		}

		static class Version2ProjectModule extends AbstractModule {
			@Override
			public void configure() {
				bind(String.class).annotatedWith(Names.named("tableName")).toInstance(Version2Project.TABLE_NAME);
			}
		}
	}

	static void launchMapReduceJob(MRWrapper mrWrapper, Provider<Scan> scanProvider,
			Class<? extends Module> customModuleClass, String tableName) {
		try {
			Configuration config = new Configuration();
			config.set(MRJobConfig.NUM_REDUCES, "0");
			config.set(MRJobConfig.REDUCE_MERGE_INMEM_THRESHOLD, "0");
			config.set(MRJobConfig.REDUCE_MEMTOMEM_ENABLED, "true");
			config.set(MRJobConfig.IO_SORT_MB, "256");
			config.set(MRJobConfig.IO_SORT_FACTOR, "100");
			config.set(MRJobConfig.JOB_UBERTASK_ENABLE, "true");
			config.set(MRJobConfig.TASK_TIMEOUT, "86400000");
			config.setInt(MRJobConfig.MAP_MEMORY_MB, 1536);
			config.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx1024M");
			config.setInt(MRJobConfig.REDUCE_MEMORY_MB, 3072);
			config.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx2560M");
			config.setClass(Job.OUTPUT_FORMAT_CLASS_ATTR, NullOutputFormat.class, OutputFormat.class);
			config.set(Constants.GUICE_CUSTOM_MODULE_ANNOTATION_STRING, customModuleClass.getName());

			Scan scan = scanProvider.get();
			scan.addFamily(Constants.FAMILY);

			mrWrapper.launchMapReduceJob(MakeColumnIndex.class.getName() + "Job", config, Optional.of(tableName),
					Optional.<String> absent(), Optional.of(scan), MakeColumnIndexMapper.class.getName(),
					Optional.<String> absent(), ImmutableBytesWritable.class, ImmutableBytesWritable.class);
		} catch (IOException | ClassNotFoundException e) {
			throw new WrappedRuntimeException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return; // Exit.
		}
	}
}
