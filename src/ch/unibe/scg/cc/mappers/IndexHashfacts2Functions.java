package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Assert;
import org.junit.Test;

import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;
import ch.unibe.scg.cc.util.HashSerializer;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class IndexHashfacts2Functions implements Runnable {
	
	@Inject
	GuiceResource resource;
	
	public static class IndexHashfacts2FunctionsMapper extends GuiceMapper {
		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		void map(ImmutableBytesWritable uselessKey, Result value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			byte[] key = value.getRow();
			assert key.length == 41;
			byte[] functionHash = Bytes.head(key, 20);
			byte[] type = Bytes.head(Bytes.tail(key, 21), 1);
			byte[] factHash = Bytes.tail(key, 20);
			byte[] factHashKey = Bytes.add(type, factHash);
			context.write(new ImmutableBytesWritable(factHashKey), new ImmutableBytesWritable(functionHash));
		}
	}
	
	public static class IndexHashfacts2FunctionsReducer extends GuiceReducer {
		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public void reduce(ImmutableBytesWritable factHashKey, Iterable<ImmutableBytesWritable> functionHashes, Context context) throws IOException, InterruptedException {
			Iterator<ImmutableBytesWritable> i = functionHashes.iterator();
			Set<byte[]> functionhashValues = resource.byteSetProvider.get();
			Set<byte[]> filecontentHashes = resource.byteSetProvider.get();
			Set<byte[]> projnameHashes = resource.byteSetProvider.get();
			Set<byte[]> versionHashes = resource.byteSetProvider.get();
			HashSerializer hashSerializer = resource.hashSerializer;
			
			while(i.hasNext()) {
				byte[] functionHash = i.next().get();
				Result row = getRow(functionHash);
				byte[] vp = row.getValue(GuiceResource.FAMILY, GuiceResource.COLUMN_VALUES_PROJECTS);
				byte[] vh = row.getValue(GuiceResource.FAMILY, GuiceResource.COLUMN_VALUES_VERSIONS);
				byte[] vf = row.getValue(GuiceResource.FAMILY, GuiceResource.COLUMN_VALUES_FILES);
				projnameHashes.addAll(hashSerializer.deserialize(vp, 20)); // XXX possible performance loss
				versionHashes.addAll(hashSerializer.deserialize(vh, 20)); // XXX possible performance loss
				filecontentHashes.addAll(hashSerializer.deserialize(vf, 20)); // XXX possible performance loss
				functionhashValues.addAll(hashSerializer.deserialize(functionHash, 20)); // XXX possible performance loss
			}
			
			Put put = resource.putFactory.create(factHashKey.get());
			put.add(GuiceResource.FAMILY, GuiceResource.COLUMN_COUNT_FUNCTIONS, 0l, Bytes.toBytes(functionhashValues.size()));
			put.add(GuiceResource.FAMILY, GuiceResource.COLUMN_VALUES_FUNCTIONS, 0l, hashSerializer.serialize(functionhashValues));
			put.add(GuiceResource.FAMILY, GuiceResource.COLUMN_COUNT_FILES, 0l, Bytes.toBytes(filecontentHashes.size()));
			put.add(GuiceResource.FAMILY, GuiceResource.COLUMN_VALUES_FILES, 0l, hashSerializer.serialize(filecontentHashes));
			put.add(GuiceResource.FAMILY, GuiceResource.COLUMN_COUNT_PROJECTS, 0l, Bytes.toBytes(projnameHashes.size()));
			put.add(GuiceResource.FAMILY, GuiceResource.COLUMN_VALUES_PROJECTS, 0l, hashSerializer.serialize(projnameHashes));
			put.add(GuiceResource.FAMILY, GuiceResource.COLUMN_COUNT_VERSIONS, 0l, Bytes.toBytes(versionHashes.size()));
			put.add(GuiceResource.FAMILY, GuiceResource.COLUMN_VALUES_VERSIONS, 0l, hashSerializer.serialize(versionHashes));
			context.write(null, put);
		}

		private Result getRow(byte[] functionHash) throws IOException {
			Get get = new Get(functionHash);
			get.addFamily(GuiceResource.FAMILY);
			return resource.indexFunctions2Files.get(get);
		}
	}

	@Override
	public void run() {
		try {
			HbaseWrapper.truncate(resource.indexHashfacts2Functions);
			
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			HbaseWrapper.launchMapReduceJob(
					IndexHashfacts2Functions.class.getName()+" Job", 
					"functions", 
					"indexHashfacts2Functions",
					scan,
					IndexHashfacts2Functions.class, 
					"IndexHashfacts2FunctionsMapper", 
					"IndexHashfacts2FunctionsReducer", 
					ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);
			
			resource.indexHashfacts2Functions.flushCommits();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static class IndexHashfacts2FunctionsTest {
		private static final byte[] FAMILY = Bytes.toBytes("d");
		
		@Test
		public void testRowScan() throws IOException {
			Injector i = Guice.createInjector(new CCModule(), new JavaModule());
			HTable indexFunctions2Files = i.getInstance(Key.get(HTable.class, Names.named("indexFunctions2Files")));
			
			Scan scan = new Scan();
//			scan.addFamily(FAMILY);
			Iterator<Result> it = indexFunctions2Files.getScanner(scan).iterator();
			Result result = it.next();
			Assert.assertTrue(!it.hasNext());
		}
	}
}
