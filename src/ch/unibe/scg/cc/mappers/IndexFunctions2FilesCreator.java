package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;
import ch.unibe.scg.cc.util.HashSerializer;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class IndexFunctions2FilesCreator implements Runnable {

	HTable indexFunctions2Files;

	@Inject
	IndexFunctions2FilesCreator(
			@Named("indexFunctions2Files") HTable indexFunctions2Files) {
		this.indexFunctions2Files = indexFunctions2Files;
	}

	public static class IndexFunctions2FilesMapper extends
			TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		public void map(ImmutableBytesWritable uselessKey, Result value,
				Context context) throws IOException, InterruptedException {
			byte[] key = value.getRow();
			assert key.length == 40;
			byte[] functionHash = Bytes.tail(key, 20);
			byte[] fileContentHash = Bytes.head(key, 20);
			context.write(new ImmutableBytesWritable(functionHash),
					new ImmutableBytesWritable(fileContentHash));
		}
	}

	public static class IndexFunctions2FilesReducer
			extends
			TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		final GuiceIndexFunctions2FilesReducer g;

		public IndexFunctions2FilesReducer() {
			Injector injector = Guice.createInjector(new CCModule(),
					new JavaModule());
			g = injector.getInstance(GuiceIndexFunctions2FilesReducer.class);
		}

		@Override
		public void reduce(ImmutableBytesWritable key,
				Iterable<ImmutableBytesWritable> values, Context context)
				throws IOException, InterruptedException {
			g.reduce(key, values, context);
		}
	}

	public static class GuiceIndexFunctions2FilesReducer
			extends
			TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		private static final byte[] FAMILY = Bytes.toBytes("d");
		private static final byte[] COLUMN_COUNT_FILES = Bytes.toBytes("nf");
		private static final byte[] COLUMN_COUNT_PROJECTS = Bytes.toBytes("np");
		private static final byte[] COLUMN_COUNT_VERSIONS = Bytes.toBytes("nv");
		private static final byte[] COLUMN_VALUES_FILES = Bytes.toBytes("vf");
		private static final byte[] COLUMN_VALUES_PROJECTS = Bytes
					.toBytes("vp");
		private static final byte[] COLUMN_VALUES_VERSIONS = Bytes
					.toBytes("vv");
		private final HTable indexFiles2Versions;
		private final HashSerializer hashSerializer;
		private final Provider<Set<byte[]>> byteSetProvider;

		@Inject
		public GuiceIndexFunctions2FilesReducer(
				@Named("indexFiles2Versions") HTable indexFiles2Versions,
				HashSerializer hashSerializer,
				Comparator<byte[]> byteArrayComparator,
				Provider<Set<byte[]>> byteSetProvider) {
			this.indexFiles2Versions = indexFiles2Versions;
			this.hashSerializer = hashSerializer;
			this.byteSetProvider = byteSetProvider;
		}

		@Override
		public void reduce(ImmutableBytesWritable functionHash,
				Iterable<ImmutableBytesWritable> fileContentHashes,
				Context context) throws IOException, InterruptedException {
			Iterator<ImmutableBytesWritable> i = fileContentHashes.iterator();
			byte[] filecontenthashValues = new byte[] {};
			Set<byte[]> projnameHashes = byteSetProvider.get();
			Set<byte[]> versionHashes = byteSetProvider.get();
			
			long filesCounter = 0;
			while (i.hasNext()) {
				byte[] cur = i.next().get();
				byte[] fileContentHash = cur;
				Result row = getRow(fileContentHash);
				byte[] pnh = getColumnValue(row, COLUMN_VALUES_PROJECTS);
				byte[] vh = getColumnValue(row, COLUMN_VALUES_VERSIONS);
				projnameHashes.addAll(hashSerializer.deserialize(pnh, 20)); // XXX possible performance loss
				versionHashes.addAll(hashSerializer.deserialize(vh, 20)); // XXX possible performance loss
				filecontenthashValues = Bytes.add(filecontenthashValues,
						fileContentHash);
				filesCounter++;
			}

			Put put = new Put(functionHash.get());
			put.add(FAMILY, COLUMN_COUNT_FILES, 0l, Bytes.toBytes(filesCounter));
			put.add(FAMILY, COLUMN_VALUES_FILES, 0l, filecontenthashValues);
			put.add(FAMILY, COLUMN_VALUES_PROJECTS, 0l, hashSerializer.serialize(projnameHashes));
			put.add(FAMILY, COLUMN_COUNT_PROJECTS, 0l, Bytes.toBytes(projnameHashes.size()));
			put.add(FAMILY, COLUMN_VALUES_VERSIONS, 0l, hashSerializer.serialize(versionHashes));
			put.add(FAMILY, COLUMN_COUNT_VERSIONS, 0l, Bytes.toBytes(versionHashes.size()));
			context.write(functionHash, put);
		}
		
		private Result getRow(byte[] fileContentHash) throws IOException {
			Iterator<Result> i = this.indexFiles2Versions.getScanner(
					new Scan(fileContentHash)).iterator();
			Result result = i.next();
			assert !i.hasNext();
			return result;
		}

		private byte[] getColumnValue(Result row, byte[] column) {
			return row.getValue(FAMILY, column);
		}
	}

	@Override
	public void run() {
		try {
			HbaseWrapper.truncate(this.indexFunctions2Files);

			Scan scan = new Scan();
			HbaseWrapper.launchMapReduceJob(
					IndexFunctions2FilesCreator.class.getName() + " Job",
					"files", "indexFunctions2Files", scan,
					IndexFunctions2FilesCreator.class,
					IndexFunctions2FilesMapper.class,
					IndexFunctions2FilesReducer.class,
					ImmutableBytesWritable.class, ImmutableBytesWritable.class);

			indexFunctions2Files.flushCommits();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static class IndexFunctions2FilesTest {

	}
}
