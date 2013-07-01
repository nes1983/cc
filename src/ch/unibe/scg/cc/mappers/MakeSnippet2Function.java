package ch.unibe.scg.cc.mappers;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.MRJobConfig;

import ch.unibe.scg.cc.WrappedRuntimeException;
import ch.unibe.scg.cc.activerecord.Column;
import ch.unibe.scg.cc.activerecord.PutFactory;

import com.google.common.base.Optional;
import com.google.common.io.BaseEncoding;

/** See paper. */
public class MakeSnippet2Function implements Runnable {
	static Logger logger = Logger.getLogger(MakeSnippet2Function.class.getName());
	private final HTable snippet2Function;
	private final MapReduceLauncher launcher;
	private final Provider<Scan> scanProvider;

	@Inject
	MakeSnippet2Function(@Named("snippet2function") HTable snippet2Function, MapReduceLauncher launcher,
			Provider<Scan> scanProvider) {
		this.snippet2Function = snippet2Function;
		this.launcher = launcher;
		this.scanProvider = scanProvider;
	}

	static class MakeSnippet2FunctionMapper extends GuiceTableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		/** receives rows from htable function2snippet */
		// super class uses unchecked types
		@Override
		public void map(ImmutableBytesWritable functionHashKey, Result value, Context context) throws IOException,
				InterruptedException {
			byte[] functionHash = functionHashKey.get();
			assert functionHash.length == 20;

			logger.finer("map " + BaseEncoding.base16().encode(functionHashKey.get()).substring(0, 4));

			NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(Column.FAMILY_NAME);

			for (Entry<byte[], byte[]> column : familyMap.entrySet()) {
				byte[] snippet = column.getKey();
				byte[] functionHashPlusLocation = Bytes.add(functionHash, column.getValue());

				logger.finer("snippet " + BaseEncoding.base16().encode(snippet).substring(0, 6) + " found");

				context.write(new ImmutableBytesWritable(snippet), new ImmutableBytesWritable(functionHashPlusLocation));
			}
		}
	}

	static class MakeSnippet2FunctionReducer extends
			GuiceTableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		final PutFactory putFactory;

		@Inject
		public MakeSnippet2FunctionReducer(@Named("snippet2function") HTableWriteBuffer snippet2Function,
				PutFactory putFactory) {
			super(snippet2Function);
			this.putFactory = putFactory;
		}

		@Override
		public void reduce(ImmutableBytesWritable snippetKey,
				Iterable<ImmutableBytesWritable> functionHashesPlusLocations, Context context) throws IOException,
				InterruptedException {
			byte[] snippet = snippetKey.get();

			logger.finer("reduce " + BaseEncoding.base16().encode(snippet).substring(0, 6));

			Put put = putFactory.create(snippet);
			int functionCount = 0;
			for (ImmutableBytesWritable functionHashPlusLocation : functionHashesPlusLocations) {
				functionCount++;

				byte[] functionHashPlusLocationKey = functionHashPlusLocation.get();
				byte[] functionHash = Bytes.head(functionHashPlusLocationKey, 20);
				byte[] factRelativeLocation = Bytes.tail(functionHashPlusLocationKey, 8);

				byte[] columnKey = ColumnKeyConverter.encode(functionHash);
				put.add(Constants.FAMILY, columnKey, 0l, factRelativeLocation);
			}

			// prevent saving non-recurring hashes
			if (functionCount > 1) {
				logger.finer("save " + BaseEncoding.base16().encode(snippet).substring(0, 6));
				context.write(snippetKey, put);
			}
		}
	}

	static class ColumnKeyConverter {
		static final int FUNCTION_LENGTH = 20;

		static byte[] encode(byte[] functionHash) {
			checkArgument(functionHash.length == FUNCTION_LENGTH, "function hash length illegally was "
					+ functionHash.length);
			return functionHash;
		}

		/** @return function hash decoded from {@code encoded} */
		static byte[] decode(byte[] encoded) {
			checkArgument(encoded.length == FUNCTION_LENGTH, "encoded length illegally was "
					+ encoded.length);
			return encoded;
		}
	}

	@Override
	public void run() {
		try {
			launcher.truncate(snippet2Function);

			Configuration config = new Configuration();
			config.set(MRJobConfig.MAP_LOG_LEVEL, "DEBUG");
			config.set(MRJobConfig.NUM_REDUCES, "30");
			// TODO test that
			config.set(MRJobConfig.REDUCE_MERGE_INMEM_THRESHOLD, "0");
			config.set(MRJobConfig.REDUCE_MEMTOMEM_ENABLED, "true");
			config.set(MRJobConfig.IO_SORT_MB, "256");
			config.set(MRJobConfig.IO_SORT_FACTOR, "100");
			config.set(MRJobConfig.JOB_UBERTASK_ENABLE, "true");
			// set to 1 if unsure TODO: check max mem allocation if only 1 jvm
			config.set(MRJobConfig.JVM_NUMTASKS_TORUN, "-1");
			config.set(MRJobConfig.TASK_TIMEOUT, "86400000");
			config.set(MRJobConfig.MAP_MEMORY_MB, "1536");
			config.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx1024M");
			config.set(MRJobConfig.REDUCE_MEMORY_MB, "3072");
			config.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx2560M");
			config.set(Constants.GUICE_CUSTOM_MODULES_ANNOTATION_STRING, HBaseModule.class.getName());

			Scan scan = scanProvider.get();
			scan.addFamily(Constants.FAMILY);

			launcher.launchMapReduceJob(MakeSnippet2Function.class.getName() + "Job", config,
					Optional.of("function2snippet"), Optional.of("snippet2function"), Optional.of(scan),
					MakeSnippet2FunctionMapper.class.getName(),
					Optional.of(MakeSnippet2FunctionReducer.class.getName()), ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);
		} catch (IOException | ClassNotFoundException e) {
			throw new WrappedRuntimeException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return; // Exit.
		}
	}
}
