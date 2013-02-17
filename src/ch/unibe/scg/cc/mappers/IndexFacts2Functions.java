package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;

import javax.inject.Inject;
import javax.inject.Named;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import ch.unibe.scg.cc.activerecord.Column;
import ch.unibe.scg.cc.activerecord.IPutFactory;
import ch.unibe.scg.cc.util.ByteUtils;
import ch.unibe.scg.cc.util.WrappedRuntimeException;

import com.google.common.base.Optional;

public class IndexFacts2Functions implements Runnable {
	static Logger logger = Logger.getLogger(IndexFacts2Functions.class);
	final HTable indexFacts2Functions;
	final MRWrapper mrWrapper;

	@Inject
	IndexFacts2Functions(@Named("indexFacts2Functions") HTable indexFacts2Files, MRWrapper mrWrapper) {
		this.indexFacts2Functions = indexFacts2Files;
		this.mrWrapper = mrWrapper;
	}

	public static class IndexFacts2FunctionsMapper<KEYOUT, VALUEOUT> extends GuiceTableMapper<KEYOUT, VALUEOUT> {
		/** receives rows from htable facts */
		@SuppressWarnings("unchecked")
		// super class uses unchecked types
		@Override
		public void map(ImmutableBytesWritable functionHashKey, Result value,
				@SuppressWarnings("rawtypes") org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException,
				InterruptedException {
			byte[] functionHash = functionHashKey.get();
			assert functionHash.length == 20;

			logger.debug("map " + ByteUtils.bytesToHex(functionHash));

			NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(Column.FAMILY_NAME);

			for (Entry<byte[], byte[]> column : familyMap.entrySet()) {
				byte[] factHash = column.getKey();
				byte[] functionHashPlusLocation = Bytes.add(functionHash, column.getValue());
				logger.debug("fact " + ByteUtils.bytesToHex(factHash) + " found");
				context.write(new ImmutableBytesWritable(factHash),
						new ImmutableBytesWritable(functionHashPlusLocation));
			}
		}
	}

	public static class IndexFacts2FunctionsReducer extends
			GuiceTableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		final IPutFactory putFactory;

		@Inject
		public IndexFacts2FunctionsReducer(@Named("indexFacts2Functions") HTable indexFacts2Functions,
				IPutFactory putFactory) {
			super(indexFacts2Functions);
			this.putFactory = putFactory;
		}

		@Override
		public void reduce(ImmutableBytesWritable factHashKey,
				Iterable<ImmutableBytesWritable> functionHashesPlusLocations, Context context) throws IOException,
				InterruptedException {
			byte[] factHash = factHashKey.get();
			Iterator<ImmutableBytesWritable> i = functionHashesPlusLocations.iterator();

			logger.debug("reduce " + ByteUtils.bytesToHex(factHash));

			Put put = putFactory.create(factHash);
			int functionCount = 0;

			while (i.hasNext()) {
				functionCount++;
				ImmutableBytesWritable functionHashPlusLocation = i.next();
				byte[] functionHashPlusLocationKey = functionHashPlusLocation.get();
				byte[] functionHash = Bytes.head(functionHashPlusLocationKey, 20);
				byte[] factRelativeLocation = Bytes.tail(functionHashPlusLocationKey, 8);

				put.add(GuiceResource.FAMILY, functionHash, 0l, factRelativeLocation);
			}

			// prevent saving non-recurring hashes
			if (functionCount > 1) {
				// TODO
				// check whether context.write(factHashKey, put) or
				// write(put) is faster
				context.write(factHashKey, put);
			}
		}
	}

	@Override
	public void run() {
		try {
			mrWrapper.truncate(indexFacts2Functions);

			Scan scan = new Scan();
			scan.setCaching(500); // TODO play with this. (100 is default value)

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

			mrWrapper.launchMapReduceJob(IndexFacts2Functions.class.getName() + "Job", config, Optional.of("facts"),
					Optional.of("indexFacts2Functions"), scan, IndexFacts2FunctionsMapper.class.getName(),
					Optional.of(IndexFacts2FunctionsReducer.class.getName()), ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);

			indexFacts2Functions.flushCommits();
		} catch (IOException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (InterruptedException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (ClassNotFoundException e) {
			throw new WrappedRuntimeException(e.getCause());
		}
	}

	public static class IndexFacts2FunctionsTest {
		@Test
		@Ignore
		public void testIndex() {
			// TODO
			Assert.assertTrue(false);
		}
	}

}
