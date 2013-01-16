package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import org.apache.commons.lang.NotImplementedException;
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

public class IndexFacts2FilesStep2 implements Runnable {
	static Logger logger = Logger.getLogger(IndexFacts2FilesStep2.class);

	static {
		logger.setLevel(Level.ALL);
	}

	final HTable indexFacts2FilesStep2;
	final HBaseWrapper hbaseWrapper;

	@Inject
	IndexFacts2FilesStep2(@Named("indexFacts2FilesStep2") HTable indexFacts2FilesStep2, HBaseWrapper hbaseWrapper) {
		this.indexFacts2FilesStep2 = indexFacts2FilesStep2;
		this.hbaseWrapper = hbaseWrapper;
	}

	public static class IndexFacts2FilesStep2Mapper<KEYOUT, VALUEOUT> extends GuiceTableMapper<KEYOUT, VALUEOUT> {

		/** receives rows from htable indexFacts2Files */
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
				byte[] fileHash = column.getKey();
				byte[] locations = column.getValue();
				byte[] factHashPlusLocations = Bytes.add(factHash, locations);

				logger.debug("file " + ByteUtils.bytesToHex(fileHash) + " found");
				context.write(new ImmutableBytesWritable(fileHash), new ImmutableBytesWritable(factHashPlusLocations));
			}
		}
	}

	public static class IndexFacts2FilesStep2Reducer extends
			GuiceTableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

		final HTable indexFacts2Files;
		final IPutFactory putFactory;
		final HashSerializer hashSerializer;
		final Provider<Set<byte[]>> byteSetProvider;

		@Inject
		public IndexFacts2FilesStep2Reducer(@Named("indexFacts2FilesStep2") HTable indexFacts2FilesStep2,
				@Named("indexFacts2Files") HTable indexFacts2Files, IPutFactory putFactory,
				HashSerializer hashSerializer, Provider<Set<byte[]>> byteSetProvider) {
			super(indexFacts2FilesStep2);
			this.indexFacts2Files = indexFacts2Files;
			this.putFactory = putFactory;
			this.hashSerializer = hashSerializer;
			this.byteSetProvider = byteSetProvider;
		}

		@Override
		public void reduce(ImmutableBytesWritable fileHashKey,
				Iterable<ImmutableBytesWritable> factHashPlusLocationsValues, Context context) throws IOException,
				InterruptedException {

			Iterator<ImmutableBytesWritable> itFactHashPlusLocations = factHashPlusLocationsValues.iterator();

			byte[] fileHash = fileHashKey.get();
			logger.info("reduce " + ByteUtils.bytesToHex(fileHash));

			Put put = putFactory.create(fileHash);

			// TODO XXX to be implemented, not finished
			if (true) {
				throw new NotImplementedException();
			}

			while (itFactHashPlusLocations.hasNext()) {
				byte[] factHashPlusLocations = itFactHashPlusLocations.next().get();
				byte[] factHash = Bytes.head(factHashPlusLocations, 20);
				byte[] locations = Bytes.tail(factHashPlusLocations, factHashPlusLocations.length - 20);
				Set<byte[]> locationSet = hashSerializer.deserialize(locations, 8);

				Result row = getRow(factHash);
				NavigableMap<byte[], byte[]> map = row.getFamilyMap(GuiceResource.FAMILY);
				for (Entry<byte[], byte[]> column : map.entrySet()) {
					// skip column where key equals fileHash
					if (Bytes.equals(fileHash, column.getKey())) {
						continue;
					}

					byte[] columnFileHash = column.getKey();
					byte[] columnLocations = column.getValue();

					// byte[] columnBytes.add(columnFileHash, factHash);
					// put.add(GuiceResource.FAMILY,
					// fileContentHashPlusLocation, 0l, new byte[] {});
				}
			}
			write(put);
		}

		private Result getRow(byte[] fileHash) throws IOException {
			Get get = new Get(fileHash);
			return indexFacts2Files.get(get);
		}
	}

	@Override
	public void run() {
		try {
			hbaseWrapper.truncate(indexFacts2FilesStep2);

			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			hbaseWrapper.launchTableMapReduceJob(IndexFacts2FilesStep2.class.getName() + " Job", "indexFacts2Files",
					"indexFacts2FilesStep2", scan, IndexFacts2FilesStep2.class, "IndexFacts2FilesStep2Mapper",
					"IndexFacts2FilesStep2Reducer", ImmutableBytesWritable.class, ImmutableBytesWritable.class,
					"-Xmx2000m");

			indexFacts2FilesStep2.flushCommits();
		} catch (IOException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (InterruptedException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (ClassNotFoundException e) {
			throw new WrappedRuntimeException(e.getCause());
		}
	}

	public static class IndexFacts2FilesTest {
		@Test
		public void testIndex() {
			// TODO
		}
	}

}
