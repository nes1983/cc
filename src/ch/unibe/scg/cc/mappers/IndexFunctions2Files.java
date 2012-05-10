package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class IndexFunctions2Files implements Runnable {

	@Inject
	GuiceResource resource;
	
	public static class IndexFunctions2FilesMapper extends GuiceMapper {
		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		void map(ImmutableBytesWritable uselessKey, Result value,
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

	public static class IndexFunctions2FilesReducer extends GuiceReducer {
		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public void reduce(ImmutableBytesWritable functionHash,
				Iterable<ImmutableBytesWritable> fileContentHashes, Context context)
				throws IOException, InterruptedException {
			Iterator<ImmutableBytesWritable> i = fileContentHashes.iterator();
			Set<byte[]> filecontentHashes = resource.byteSetProvider.get();
			Set<byte[]> projnameHashes = resource.byteSetProvider.get();
			Set<byte[]> versionHashes = resource.byteSetProvider.get();
			
			while (i.hasNext()) {
				byte[] cur = i.next().get();
				byte[] fileContentHash = cur;
				Result row = getRow(fileContentHash);
				byte[] pnh = getColumnValue(row, GuiceResource.COLUMN_VALUES_PROJECTS);
				byte[] vh = getColumnValue(row, GuiceResource.COLUMN_VALUES_VERSIONS);
				projnameHashes.addAll(resource.hashSerializer.deserialize(pnh, 20)); // XXX possible performance loss
				versionHashes.addAll(resource.hashSerializer.deserialize(vh, 20)); // XXX possible performance loss
				filecontentHashes.addAll(resource.hashSerializer.deserialize(fileContentHash, 20)); // XXX possible performance loss
			}

			Put put = new Put(functionHash.get());
			put.add(GuiceResource.FAMILY, GuiceResource.COLUMN_COUNT_FILES, 0l, Bytes.toBytes(filecontentHashes.size()));
			put.add(GuiceResource.FAMILY, GuiceResource.COLUMN_VALUES_FILES, 0l, resource.hashSerializer.serialize(filecontentHashes));
			put.add(GuiceResource.FAMILY, GuiceResource.COLUMN_COUNT_PROJECTS, 0l, Bytes.toBytes(projnameHashes.size()));
			put.add(GuiceResource.FAMILY, GuiceResource.COLUMN_VALUES_PROJECTS, 0l, resource.hashSerializer.serialize(projnameHashes));
			put.add(GuiceResource.FAMILY, GuiceResource.COLUMN_COUNT_VERSIONS, 0l, Bytes.toBytes(versionHashes.size()));
			put.add(GuiceResource.FAMILY, GuiceResource.COLUMN_VALUES_VERSIONS, 0l, resource.hashSerializer.serialize(versionHashes));
			context.write(functionHash, put);
		}
		
		private Result getRow(byte[] fileContentHash) throws IOException {
			Iterator<Result> i = resource.indexFiles2Versions.getScanner(
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
			HbaseWrapper.truncate(resource.indexFunctions2Files);

			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			HbaseWrapper.launchMapReduceJob(
					IndexFunctions2Files.class.getName() + " Job",
					"files", "indexFunctions2Files", scan,
					IndexFunctions2Files.class,
					"IndexFunctions2FilesMapper",
					"IndexFunctions2FilesReducer",
					ImmutableBytesWritable.class, ImmutableBytesWritable.class);

			resource.indexFunctions2Files.flushCommits();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static class IndexFunctions2FilesTest {

	}
}
