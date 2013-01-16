package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import ch.unibe.scg.cc.activerecord.Column;
import ch.unibe.scg.cc.activerecord.IPutFactory;
import ch.unibe.scg.cc.util.ByteUtils;
import ch.unibe.scg.cc.util.HashSerializer;
import ch.unibe.scg.cc.util.WrappedRuntimeException;

public class IndexFacts2Files implements Runnable {
	static Logger logger = Logger.getLogger(IndexFacts2Files.class);

	static {
		logger.setLevel(Level.ALL);
	}

	final HTable indexFacts2Files;
	final HBaseWrapper hbaseWrapper;

	@Inject
	IndexFacts2Files(@Named("indexFacts2Files") HTable indexFacts2Files, HBaseWrapper hbaseWrapper) {
		this.indexFacts2Files = indexFacts2Files;
		this.hbaseWrapper = hbaseWrapper;
	}

	public static class IndexFacts2FilesMapper<KEYOUT, VALUEOUT> extends GuiceTableMapper<KEYOUT, VALUEOUT> {

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

	public static class IndexFacts2FilesReducer extends
			GuiceTableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

		final HTable functions;
		final IPutFactory putFactory;
		final HashSerializer hashSerializer;
		final Provider<Set<byte[]>> byteSetProvider;

		@Inject
		public IndexFacts2FilesReducer(@Named("indexFacts2Files") HTable indexFacts2Files,
				@Named("functions") HTable functions, IPutFactory putFactory, HashSerializer hashSerializer,
				Provider<Set<byte[]>> byteSetProvider) {
			super(indexFacts2Files);
			this.functions = functions;
			this.putFactory = putFactory;
			this.hashSerializer = hashSerializer;
			this.byteSetProvider = byteSetProvider;
		}

		@Override
		public void reduce(ImmutableBytesWritable factHashKey,
				Iterable<ImmutableBytesWritable> functionHashesPlusLocations, Context context) throws IOException,
				InterruptedException {

			byte[] factHash = factHashKey.get();
			Iterator<ImmutableBytesWritable> i = functionHashesPlusLocations.iterator();

			logger.debug("reduce " + ByteUtils.bytesToHex(factHash));

			Put put = putFactory.create(factHash);
			int fileCount = 0;

			while (i.hasNext()) {
				ImmutableBytesWritable functionHashPlusLocation = i.next();
				byte[] functionHashPlusLocationKey = functionHashPlusLocation.get();
				byte[] functionHash = Bytes.head(functionHashPlusLocationKey, 20);

				byte[] factRelativeLocation = Bytes.tail(functionHashPlusLocationKey, 8);
				byte[] factRelativePosition = Bytes.head(factRelativeLocation, 4);
				byte[] factLength = Bytes.tail(factRelativeLocation, 4);

				logger.debug("scan function " + ByteUtils.bytesToHex(functionHash));
				ResultScanner rowsScanner = getRows(functionHash);

				Iterator<Result> iterator = rowsScanner.iterator();
				Hashtable<byte[], Set<byte[]>> columns = new Hashtable<byte[], Set<byte[]>>();
				while (iterator.hasNext()) {
					fileCount++;
					Result function = iterator.next();
					byte[] fileHash = function.getRow();
					logger.debug("file " + ByteUtils.bytesToHex(fileHash) + " found");

					byte[] functionPosition = function.getValue(GuiceResource.FAMILY, functionHash);
					int factAbsolutePosition = Bytes.toInt(functionPosition) + Bytes.toInt(factRelativePosition);
					byte[] location = Bytes.add(Bytes.toBytes(factAbsolutePosition), factLength);

					Set<byte[]> locations;
					if (columns.containsKey(fileHash)) {
						locations = columns.get(fileHash);
					} else {
						locations = new HashSet<byte[]>();
						columns.put(fileHash, locations);
					}
					locations.add(location);
				}

				for (Entry<byte[], Set<byte[]>> column : columns.entrySet()) {
					put.add(GuiceResource.FAMILY, column.getKey(), 0l, hashSerializer.serialize(column.getValue()));
				}
			}

			// prevent saving non-recurring hashes
			if (fileCount > 1) {
				write(put);
			}
		}

		/**
		 * Creates a {@link ResultScanner} with a {@link QualifierFilter} to
		 * match columns where the key equals the input parameter functionHash
		 * 
		 * @return {@link ResultScanner} with columns from the table functions.
		 */
		private ResultScanner getRows(byte[] functionHash) throws IOException {
			WritableByteArrayComparable qualifierComparator = new BinaryComparator(functionHash);
			Filter filter = new QualifierFilter(CompareOp.EQUAL, qualifierComparator);
			Scan scan = new Scan();
			scan.setFilter(filter);
			scan.addFamily(GuiceResource.FAMILY);
			return functions.getScanner(scan);
		}
	}

	@Override
	public void run() {
		try {
			hbaseWrapper.truncate(indexFacts2Files);

			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			hbaseWrapper.launchTableMapReduceJob(IndexFacts2Files.class.getName() + " Job", "facts",
					"indexFacts2Files", scan, IndexFacts2Files.class, "IndexFacts2FilesMapper",
					"IndexFacts2FilesReducer", ImmutableBytesWritable.class, ImmutableBytesWritable.class, "-Xmx2000m");

			indexFacts2Files.flushCommits();
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
