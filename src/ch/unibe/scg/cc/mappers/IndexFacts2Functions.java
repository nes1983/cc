package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;

import javax.inject.Inject;
import javax.inject.Named;

import junit.framework.Assert;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import ch.unibe.scg.cc.activerecord.Column;
import ch.unibe.scg.cc.activerecord.IPutFactory;
import ch.unibe.scg.cc.util.ByteUtils;
import ch.unibe.scg.cc.util.WrappedRuntimeException;

public class IndexFacts2Functions implements Runnable {
	static Logger logger = Logger.getLogger(IndexFacts2Functions.class);

	static {
		logger.setLevel(Level.ALL);
	}

	final HTable indexFacts2Functions;
	final HBaseWrapper hbaseWrapper;

	@Inject
	IndexFacts2Functions(@Named("indexFacts2Functions") HTable indexFacts2Files, HBaseWrapper hbaseWrapper) {
		this.indexFacts2Functions = indexFacts2Files;
		this.hbaseWrapper = hbaseWrapper;
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
			hbaseWrapper.truncate(indexFacts2Functions);

			Scan scan = new Scan();
			scan.setCaching(500); // TODO play with this. (100 is default value)
			scan.setCacheBlocks(false);
			hbaseWrapper.launchTableMapReduceJob(IndexFacts2Functions.class.getName() + " Job", "facts",
					"indexFacts2Functions", scan, IndexFacts2Functions.class, "IndexFacts2FunctionsMapper",
					"IndexFacts2FunctionsReducer", ImmutableBytesWritable.class, ImmutableBytesWritable.class,
					"-Xmx2000m");

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
		public void testIndex() {
			// TODO
			Assert.assertTrue(false);
		}
	}

}
