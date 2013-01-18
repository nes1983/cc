package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;

import javax.inject.Inject;
import javax.inject.Named;

import junit.framework.Assert;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import ch.unibe.scg.cc.activerecord.IPutFactory;
import ch.unibe.scg.cc.util.ByteUtils;
import ch.unibe.scg.cc.util.HashSerializer;
import ch.unibe.scg.cc.util.WrappedRuntimeException;

public class IndexFacts2FunctionsStep2 implements Runnable {
	static Logger logger = Logger.getLogger(IndexFacts2FunctionsStep2.class);

	static {
		logger.setLevel(Level.ALL);
	}

	final HTable indexFacts2FunctionsStep2;
	final HBaseWrapper hbaseWrapper;

	@Inject
	IndexFacts2FunctionsStep2(@Named("indexFacts2FunctionsStep2") HTable indexFacts2FilesStep2,
			HBaseWrapper hbaseWrapper) {
		this.indexFacts2FunctionsStep2 = indexFacts2FilesStep2;
		this.hbaseWrapper = hbaseWrapper;
	}

	public static class IndexFacts2FunctionsStep2Mapper<KEYOUT, VALUEOUT> extends GuiceTableMapper<KEYOUT, VALUEOUT> {
		/** receives rows from htable indexFacts2Functions */
		@SuppressWarnings("unchecked")
		@Override
		public void map(ImmutableBytesWritable uselessKey, Result value,
				@SuppressWarnings("rawtypes") org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException,
				InterruptedException {
			byte[] factHash = value.getRow();
			assert factHash.length == 20;

			logger.debug("map fact " + ByteUtils.bytesToHex(factHash));

			NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(GuiceResource.FAMILY);

			for (Entry<byte[], byte[]> column : familyMap.entrySet()) {
				byte[] functionHash = column.getKey();
				byte[] location = column.getValue();
				byte[] factHashPlusLocation = Bytes.add(factHash, location);

				logger.debug("function " + ByteUtils.bytesToHex(functionHash) + " found");
				context.write(new ImmutableBytesWritable(functionHash),
						new ImmutableBytesWritable(factHashPlusLocation));
			}
		}
	}

	public static class IndexFacts2FunctionsStep2Reducer extends
			GuiceTableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		final HTable indexFacts2Functions;
		final IPutFactory putFactory;
		final HashSerializer hashSerializer;

		@Inject
		public IndexFacts2FunctionsStep2Reducer(@Named("indexFacts2FunctionsStep2") HTable indexFacts2FilesStep2,
				@Named("indexFacts2Functions") HTable indexFacts2Functions, IPutFactory putFactory,
				HashSerializer hashSerializer) {
			super(indexFacts2FilesStep2);
			this.indexFacts2Functions = indexFacts2Functions;
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
				byte[] factHashPlusLocation = itFactHashPlusLocation.next().get();
				byte[] factHash = Bytes.head(factHashPlusLocation, 21); // type(1byte)+fact(20bytes)
				byte[] location = Bytes.tail(factHashPlusLocation, 8);
				byte[] position = Bytes.head(location, 4);
				byte[] putColumnKey = Bytes.add(position, factHash);
				assert putColumnKey.length == 25; // 4B pos + 1B type + 20B hash

				Result row = getRow(factHash);
				NavigableMap<byte[], byte[]> map = row.getFamilyMap(GuiceResource.FAMILY);
				for (Entry<byte[], byte[]> column : map.entrySet()) {
					// skip column where key equals fileHash
					if (Bytes.equals(functionHash, column.getKey())) {
						continue;
					}

					byte[] columnFunctionHash = column.getKey();
					byte[] columnLocation = column.getValue();
					byte[] putColumnValue = Bytes.add(columnFunctionHash, columnLocation);

					put.add(GuiceResource.FAMILY, putColumnKey, 0l, putColumnValue);
				}
			}
			context.write(functionHashKey, put);
		}

		private Result getRow(byte[] functionHash) throws IOException {
			Get get = new Get(functionHash);
			return indexFacts2Functions.get(get);
		}
	}

	@Override
	public void run() {
		try {
			hbaseWrapper.truncate(indexFacts2FunctionsStep2);

			Scan scan = new Scan();
			scan.setCaching(500); // TODO play with this. (100 is default value)
			scan.setCacheBlocks(false);
			hbaseWrapper.launchTableMapReduceJob(IndexFacts2FunctionsStep2.class.getName() + " Job",
					"indexFacts2Functions", "indexFacts2FunctionsStep2", scan, IndexFacts2FunctionsStep2.class,
					"IndexFacts2FunctionsStep2Mapper", "IndexFacts2FunctionsStep2Reducer",
					ImmutableBytesWritable.class, ImmutableBytesWritable.class, "-Xmx2000m");

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
		public void testIndex() {
			// TODO
			Assert.assertTrue(false);
		}
	}

}
