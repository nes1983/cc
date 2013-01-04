package ch.unibe.scg.cc.mappers;

import java.io.IOException;
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
import org.apache.log4j.Logger;

import ch.unibe.scg.cc.activerecord.Column;
import ch.unibe.scg.cc.activerecord.IPutFactory;
import ch.unibe.scg.cc.util.HashSerializer;
import ch.unibe.scg.cc.util.WrappedRuntimeException;

public class IndexFacts2Files implements Runnable {
	static Logger logger = Logger.getLogger(IndexFacts2Files.class);

	final HTable indexFacts2Files;
	final HBaseWrapper hbaseWrapper;

	@Inject
	IndexFacts2Files(@Named("indexFacts2Files") HTable indexFacts2Files,
			HBaseWrapper hbaseWrapper) {
		this.indexFacts2Files = indexFacts2Files;
		this.hbaseWrapper = hbaseWrapper;
	}

	public static class IndexFacts2FilesMapper<KEYOUT, VALUEOUT> extends
			GuiceTableMapper<KEYOUT, VALUEOUT> {
		@Override
		public void map(ImmutableBytesWritable uselessKey, Result value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			byte[] functionHash = value.getRow();
			assert functionHash.length == 20;

			logger.debug("map " + functionHash);

			NavigableMap<byte[], byte[]> familyMap = value
					.getFamilyMap(Column.FAMILY_NAME);

			for (Entry<byte[], byte[]> column : familyMap.entrySet()) {
				byte[] functionHashPlusLocation = Bytes.add(functionHash,
						column.getValue());
				logger.debug("--> " + column.getKey());
				context.write(new ImmutableBytesWritable(column.getKey()),
						new ImmutableBytesWritable(functionHashPlusLocation));
			}
		}
	}

	public static class IndexFacts2FilesReducer
			extends
			GuiceTableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

		final HTable functions;
		final IPutFactory putFactory;
		final HashSerializer hashSerializer;
		final Provider<Set<byte[]>> byteSetProvider;

		@Inject
		public IndexFacts2FilesReducer(
				@Named("indexFacts2Files") HTable indexFacts2Files,
				@Named("functions") HTable functions, IPutFactory putFactory,
				HashSerializer hashSerializer,
				Provider<Set<byte[]>> byteSetProvider) {
			super(indexFacts2Files);
			this.functions = functions;
			this.putFactory = putFactory;
			this.hashSerializer = hashSerializer;
			this.byteSetProvider = byteSetProvider;
		}

		@Override
		public void reduce(ImmutableBytesWritable factHashKey,
				Iterable<ImmutableBytesWritable> functionHashesPlusLocations,
				Context context) throws IOException, InterruptedException {

			Iterator<ImmutableBytesWritable> i = functionHashesPlusLocations
					.iterator();

			logger.debug("reduce " + factHashKey.get());

			while (i.hasNext()) {
				ImmutableBytesWritable value = i.next();
				byte[] valueKey = value.get();
				byte[] functionHash = Bytes.head(valueKey, 20);
				byte[] locations = Bytes.tail(valueKey, 8);

				Put put = putFactory.create(factHashKey.get());

				logger.debug("--> " + functionHash);
				logger.debug("scan " + bytesToHex(functionHash));
				ResultScanner rowsScanner = getRows(functionHash);
				Iterator<Result> iterator = rowsScanner.iterator();
				while (iterator.hasNext()) {
					Result result = iterator.next();
					byte[] fileContentHash = result.getRow();
					byte[] fileContentHashPlusLocation = Bytes.add(
							fileContentHash, locations);
					put.add(GuiceResource.FAMILY, fileContentHashPlusLocation,
							0l, new byte[] {});

					logger.debug("----> " + fileContentHash);
				}
				logger.debug("write put");
				context.write(factHashKey, put);
			}
		}

		private ResultScanner getRows(byte[] functionHash) throws IOException {
			WritableByteArrayComparable qualifierComparator = new BinaryComparator(
					functionHash);
			Filter filter = new QualifierFilter(CompareOp.EQUAL,
					qualifierComparator);
			Scan scan = new Scan();
			scan.setFilter(filter);
			scan.addFamily(GuiceResource.FAMILY);
			return functions.getScanner(scan);
		}

		public static String bytesToHex(byte[] bytes) {
			final char[] hexArray = { '0', '1', '2', '3', '4', '5', '6', '7',
					'8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
			char[] hexChars = new char[bytes.length * 2];
			int v;
			for (int j = 0; j < bytes.length; j++) {
				v = bytes[j] & 0xFF;
				hexChars[j * 2] = hexArray[v >>> 4];
				hexChars[j * 2 + 1] = hexArray[v & 0x0F];
			}
			return new String(hexChars);
		}
	}

	@Override
	public void run() {
		try {
			hbaseWrapper.truncate(indexFacts2Files);

			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			hbaseWrapper.launchTableMapReduceJob(
					IndexFacts2Files.class.getName() + " Job", "facts",
					"indexFacts2Files", scan, IndexFacts2Files.class,
					"IndexFacts2FilesMapper", "IndexFacts2FilesReducer",
					ImmutableBytesWritable.class, ImmutableBytesWritable.class,
					"-Xmx2000m");

			indexFacts2Files.flushCommits();
		} catch (IOException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (InterruptedException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (ClassNotFoundException e) {
			throw new WrappedRuntimeException(e.getCause());
		}
	}
}
