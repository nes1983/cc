package ch.unibe.scg.cc.mappers;

import java.io.IOException;
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
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.util.HashSerializer;
import ch.unibe.scg.cc.util.WrappedRuntimeException;

public class IndexFunctions2Files implements Runnable {

	final HTable indexFunctions2Files;
	final HbaseWrapper hbaseWrapper;
	
	@Inject
	IndexFunctions2Files(@Named("indexFunctions2Files") HTable indexFunctions2Files, HbaseWrapper hbaseWrapper) {
		this.indexFunctions2Files = indexFunctions2Files;
		this.hbaseWrapper = hbaseWrapper;
	}
	
	public static class IndexFunctions2FilesMapper<KEYOUT,VALUEOUT> extends GuiceTableMapper<KEYOUT,VALUEOUT> {
		@Override
		public void map(ImmutableBytesWritable uselessKey, Result value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			byte[] key = value.getRow();
			assert key.length == 40;
			byte[] functionHash = Bytes.tail(key, 20);
			byte[] fileContentHash = Bytes.head(key, 20);
			context.write(new ImmutableBytesWritable(functionHash),
					new ImmutableBytesWritable(fileContentHash));
		}
	}

	public static class IndexFunctions2FilesReducer extends GuiceTableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		final HashSerializer hashSerializer;
		final Provider<Set<byte[]>> byteSetProvider;
		final HTable indexFiles2Versions;

		@Inject
		IndexFunctions2FilesReducer(
				@Named("indexFunctions2Files") HTable indexFunctions2Files,
				@Named("indexFiles2Versions") HTable indexFiles2Versions,
				HashSerializer hashSerializer,
				Provider<Set<byte[]>> byteSetProvider) {
			super(indexFunctions2Files);
			this.indexFiles2Versions = indexFiles2Versions;
			this.hashSerializer = hashSerializer;
			this.byteSetProvider = byteSetProvider;
		}

		@Override
		public void reduce(ImmutableBytesWritable functionHash,
				Iterable<ImmutableBytesWritable> fileContentHashes, Context context)
				throws IOException, InterruptedException {
			Iterator<ImmutableBytesWritable> i = fileContentHashes.iterator();
			Set<byte[]> filecontentHashes = byteSetProvider.get();
			Set<byte[]> projnameHashes = byteSetProvider.get();
			Set<byte[]> versionHashes = byteSetProvider.get();
			
			while (i.hasNext()) {
				byte[] cur = i.next().get();
				byte[] fileContentHash = cur;
				Result row = getRow(fileContentHash);
				byte[] pnh = getColumnValue(row, GuiceResource.ColumnName.VALUES_VERSIONS.getName());
				byte[] vh = getColumnValue(row, GuiceResource.ColumnName.VALUES_FILES.getName());
				projnameHashes.addAll(hashSerializer.deserialize(pnh, 20)); // XXX possible performance loss
				versionHashes.addAll(hashSerializer.deserialize(vh, 20)); // XXX possible performance loss
				filecontentHashes.addAll(hashSerializer.deserialize(fileContentHash, 20)); // XXX possible performance loss
			}

			Put put = new Put(functionHash.get());
			put.add(GuiceResource.FAMILY, GuiceResource.ColumnName.COUNT_FUNCTIONS.getName(), 0l, Bytes.toBytes(filecontentHashes.size()));
			put.add(GuiceResource.FAMILY, GuiceResource.ColumnName.VALUES_FUNCTIONS.getName(), 0l, hashSerializer.serialize(filecontentHashes));
			put.add(GuiceResource.FAMILY, GuiceResource.ColumnName.COUNT_VERSIONS.getName(), 0l, Bytes.toBytes(projnameHashes.size()));
			put.add(GuiceResource.FAMILY, GuiceResource.ColumnName.VALUES_VERSIONS.getName(), 0l, hashSerializer.serialize(projnameHashes));
			put.add(GuiceResource.FAMILY, GuiceResource.ColumnName.COUNT_FILES.getName(), 0l, Bytes.toBytes(versionHashes.size()));
			put.add(GuiceResource.FAMILY, GuiceResource.ColumnName.VALUES_FILES.getName(), 0l, hashSerializer.serialize(versionHashes));
			context.write(functionHash, put);
		}
		
		private Result getRow(byte[] fileContentHash) throws IOException {
			Iterator<Result> i = indexFiles2Versions.getScanner(
					new Scan(fileContentHash)).iterator();
			Result result = i.next();
			assert !i.hasNext();
			return result;
		}

		private byte[] getColumnValue(Result row, byte[] column) {
			return row.getValue(GuiceResource.FAMILY, column);
		}
	}

	@Override
	public void run() {
		try {
			hbaseWrapper.truncate(indexFunctions2Files);

			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			hbaseWrapper.launchTableMapReduceJob(
					IndexFunctions2Files.class.getName() + " Job",
					"files", "indexFunctions2Files", scan,
					IndexFunctions2Files.class,
					"IndexFunctions2FilesMapper",
					"IndexFunctions2FilesReducer",
					ImmutableBytesWritable.class, ImmutableBytesWritable.class);

			indexFunctions2Files.flushCommits();
		} catch (IOException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (InterruptedException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (ClassNotFoundException e) {
			throw new WrappedRuntimeException(e.getCause());
		}
	}

	public static class IndexFunctions2FilesTest {

	}
}
