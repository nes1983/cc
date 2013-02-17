package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

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

import ch.unibe.scg.cc.activerecord.IPutFactory;
import ch.unibe.scg.cc.mappers.Protos.FunctionLocation;
import ch.unibe.scg.cc.util.ByteUtils;
import ch.unibe.scg.cc.util.HashSerializer;
import ch.unibe.scg.cc.util.WrappedRuntimeException;

import com.google.common.base.Optional;
import com.google.protobuf.ByteString;

public class IndexFacts2FunctionsStep2 implements Runnable {
	static Logger logger = Logger.getLogger(IndexFacts2FunctionsStep2.class);
	final HTable indexFacts2FunctionsStep2;
	final MRWrapper mrWrapper;

	@Inject
	IndexFacts2FunctionsStep2(@Named("indexFacts2FunctionsStep2") HTable indexFacts2FilesStep2, MRWrapper mrWrapper) {
		this.indexFacts2FunctionsStep2 = indexFacts2FilesStep2;
		this.mrWrapper = mrWrapper;
	}

	/**
	 * INPUT:<br>
	 * 
	 * <pre>
	 * FAC1 --> { [F1|2] , [F2|3] , [F3|8] }
	 * FAC2 --> { [F1|3] , [F3|9] }
	 * </pre>
	 * 
	 * OUTPUT:<br>
	 * 
	 * <pre>
	 * F1 --> { [F2,2|FAC1,3], [F3,2|FAC1,8], [F3,3|FAC2,9] }
	 * F2 --> { [F1,3|FAC1,2], [F3,3|FAC1,8] }
	 * F3 --> { [F1,8|FAC1,2], [F1,9|FAC2,9], [F2,8|FAC1,3] }
	 * </pre>
	 */
	public static class IndexFacts2FunctionsStep2Mapper extends
			GuiceTableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		/** receives rows from htable indexFacts2Functions */
		@SuppressWarnings("unchecked")
		@Override
		public void map(ImmutableBytesWritable uselessKey, Result value,
				@SuppressWarnings("rawtypes") org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException,
				InterruptedException {
			byte[] factHash = value.getRow();
			assert factHash.length == 20;

			logger.debug("map fact " + ByteUtils.bytesToHex(factHash));

			// factHash = new byte[] {}; // dummy to save space

			NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(GuiceResource.FAMILY);
			if (familyMap.size() > 1000) {
				logger.warn("FAMILY MAP SIZE " + familyMap.size());
			}
			Set<Entry<byte[], byte[]>> columns = familyMap.entrySet();
			Iterator<Entry<byte[], byte[]>> columnIterator = columns.iterator();
			// cross product of the columns of the factHash
			while (columnIterator.hasNext()) {
				Entry<byte[], byte[]> columnFixed = columnIterator.next();
				byte[] function = columnFixed.getKey();
				byte[] rowFunctionLocation = columnFixed.getValue();
				for (Entry<byte[], byte[]> columnVar : columns) {
					if (columnFixed.equals(columnVar)) {
						continue;
					}

					byte[] factHashLocation = columnVar.getValue();
					FunctionLocation loc = FunctionLocation.newBuilder().setFunction(ByteString.copyFrom(function))
							.setRowFunctionLocation(ByteString.copyFrom(rowFunctionLocation))
							.setFactHash(ByteString.copyFrom(factHash))
							.setFactHashLocation(ByteString.copyFrom(factHashLocation)).build();
					context.write(new ImmutableBytesWritable(function), new ImmutableBytesWritable(loc.toByteArray()));
				}
			}
		}
	}

	public static class IndexFacts2FunctionsStep2Reducer extends
			GuiceTableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		final IPutFactory putFactory;
		final HashSerializer hashSerializer;

		@Inject
		public IndexFacts2FunctionsStep2Reducer(@Named("indexFacts2FunctionsStep2") HTable indexFacts2FunctionsStep2,
				IPutFactory putFactory, HashSerializer hashSerializer) {
			super(indexFacts2FunctionsStep2);
			this.putFactory = putFactory;
			this.hashSerializer = hashSerializer;
		}

		@Override
		public void reduce(ImmutableBytesWritable functionHashKey,
				Iterable<ImmutableBytesWritable> factHashPlusLocationValues, Context context) throws IOException,
				InterruptedException {
			Iterator<ImmutableBytesWritable> itFactHashPlusLocation = factHashPlusLocationValues.iterator();

			byte[] functionHash = functionHashKey.get();
			logger.info("reduce " + ByteUtils.bytesToHex(functionHash));

			Put put = putFactory.create(functionHash);

			while (itFactHashPlusLocation.hasNext()) {
				FunctionLocation fl = FunctionLocation.parseFrom(itFactHashPlusLocation.next().get());
				byte[] columnName = Bytes
						.add(fl.getFunction().toByteArray(), fl.getRowFunctionLocation().toByteArray());
				byte[] columnValue = Bytes.add(fl.getFactHash().toByteArray(), fl.getFactHashLocation().toByteArray());
				put.add(GuiceResource.FAMILY, columnName, 0l, columnValue);
			}
			context.write(functionHashKey, put);
		}
	}

	@Override
	public void run() {
		try {
			mrWrapper.truncate(indexFacts2FunctionsStep2);

			Scan scan = new Scan();
			scan.setCaching(100); // TODO play with this. (100 is default value)
			scan.setCacheBlocks(false);
			scan.addFamily(GuiceResource.FAMILY); // Gets all columns from the
													// specified family.

			Configuration config = new Configuration();
			config.set(MRJobConfig.MAP_LOG_LEVEL, "DEBUG");
			config.set(MRJobConfig.NUM_REDUCES, "30");
			// TODO test that
			config.set(MRJobConfig.REDUCE_MERGE_INMEM_THRESHOLD, "0");
			config.set(MRJobConfig.REDUCE_MEMTOMEM_ENABLED, "true");
			config.set(MRJobConfig.IO_SORT_MB, "512");
			config.set(MRJobConfig.IO_SORT_FACTOR, "100");
			config.set(MRJobConfig.JOB_UBERTASK_ENABLE, "true");
			// set to 1 if unsure TODO: check max mem allocation if only 1 jvm
			config.set(MRJobConfig.JVM_NUMTASKS_TORUN, "-1");
			config.set(MRJobConfig.TASK_TIMEOUT, "86400000");
			config.set(MRJobConfig.MAP_MEMORY_MB, "1536");
			config.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx1024M");
			config.set(MRJobConfig.REDUCE_MEMORY_MB, "3072");
			config.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx2560M");

			mrWrapper.launchMapReduceJob(IndexFacts2FunctionsStep2.class.getName() + "Job", config,
					Optional.of("indexFacts2Functions"), Optional.of("indexFacts2FunctionsStep2"), scan,
					IndexFacts2FunctionsStep2Mapper.class.getName(),
					Optional.of(IndexFacts2FunctionsStep2Reducer.class.getName()), ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);

			indexFacts2FunctionsStep2.flushCommits();
		} catch (IOException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (InterruptedException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (ClassNotFoundException e) {
			throw new WrappedRuntimeException(e.getCause());
		}
	}

	public static class IndexFacts2FunctionsStep2Test {
		@Test
		@Ignore
		public void testIndex() {
			// TODO
			Assert.assertTrue(false);
		}
	}

}
