package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;
import ch.unibe.scg.cc.util.HashSerializer;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class IndexHashfacts2FunctionsCreator implements Runnable {
	
	HTable indexHashfacts2Functions;
	
	@Inject
	IndexHashfacts2FunctionsCreator(@Named("indexHashfacts2Functions") HTable indexHashfacts2Functions) {
		this.indexHashfacts2Functions = indexHashfacts2Functions;
	}

	public static class IndexHashfacts2FunctionsMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		public void map(ImmutableBytesWritable uselessKey, Result value, Context context) throws IOException, InterruptedException {
			byte[] key = value.getRow();
			assert key.length == 41;
			byte[] functionHash = Bytes.head(key, 20);
			byte[] type = Bytes.head(Bytes.tail(key, 21), 1);
			byte[] factHash = Bytes.tail(key, 20);
			byte[] factHashKey = Bytes.add(type, factHash);
			context.write(new ImmutableBytesWritable(factHashKey), new ImmutableBytesWritable(functionHash));
		}
	}
	
	public static class IndexHashfacts2FunctionsReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		final GuiceIndexHashfacts2FunctionsReducer g;
		
		public IndexHashfacts2FunctionsReducer() {
			Injector injector = Guice.createInjector(new CCModule(), new JavaModule());
			g = injector.getInstance(GuiceIndexHashfacts2FunctionsReducer.class);
		}
		
		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
			g.reduce(key, values, context);
		}
	}
	
	public static class GuiceIndexHashfacts2FunctionsReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		private static final byte[] FAMILY = Bytes.toBytes("d");
		private static final byte[] COLUMN_COUNT_FUNCTIONS = Bytes.toBytes("nf");
		private static final byte[] COLUMN_VALUES_FUNCTIONS = Bytes.toBytes("vf");
		private static final byte[] COLUMN_COUNT_FILES = Bytes.toBytes("nf");
		private static final byte[] COLUMN_COUNT_PROJECTS = Bytes.toBytes("np");
		private static final byte[] COLUMN_COUNT_VERSIONS = Bytes.toBytes("nv");
		private static final byte[] COLUMN_VALUES_FILES = Bytes.toBytes("vf");
		private static final byte[] COLUMN_VALUES_PROJECTS = Bytes.toBytes("vp");
		private static final byte[] COLUMN_VALUES_VERSIONS = Bytes.toBytes("vv");
		private final HTable indexFunctions2Files;
		private HashSerializer hashSerializer;
		private Provider<Set<byte[]>> byteSetProvider;
		
		@Inject
		public GuiceIndexHashfacts2FunctionsReducer(@Named("indexFunctions2Files") HTable indexFunctions2Files,
				HashSerializer hashSerializer,
				Provider<Set<byte[]>> byteSetProvider) {
			this.indexFunctions2Files = indexFunctions2Files;
			this.hashSerializer = hashSerializer;
			this.byteSetProvider = byteSetProvider;
		}

		@Override
		public void reduce(ImmutableBytesWritable factHashKey, Iterable<ImmutableBytesWritable> functionHashes, Context context) throws IOException, InterruptedException {
			Iterator<ImmutableBytesWritable> i = functionHashes.iterator();
			Set<byte[]> functionhashValues = byteSetProvider.get();
			Set<byte[]> filecontentHashes = byteSetProvider.get();
			Set<byte[]> projnameHashes = byteSetProvider.get();
			Set<byte[]> versionHashes = byteSetProvider.get();
			while(i.hasNext()) {
				byte[] functionHash = i.next().get();
				Result row = getRow(functionHash);
				byte[] vp = row.getValue(FAMILY, COLUMN_VALUES_PROJECTS);
				byte[] vh = row.getValue(FAMILY, COLUMN_VALUES_VERSIONS);
				byte[] vf = row.getValue(FAMILY, COLUMN_VALUES_FILES);
				projnameHashes.addAll(hashSerializer.deserialize(vp, 20)); // XXX possible performance loss
				versionHashes.addAll(hashSerializer.deserialize(vh, 20)); // XXX possible performance loss
				filecontentHashes.addAll(hashSerializer.deserialize(vf, 20)); // XXX possible performance loss
				functionhashValues.addAll(hashSerializer.deserialize(functionHash, 20)); // XXX possible performance loss
			}
			
			Put put = new Put(factHashKey.get());
			put.add(FAMILY, COLUMN_COUNT_FUNCTIONS, 0l, Bytes.toBytes(functionhashValues.size()));
			put.add(FAMILY, COLUMN_VALUES_FUNCTIONS, 0l, hashSerializer.serialize(functionhashValues));
			put.add(FAMILY, COLUMN_COUNT_FILES, 0l, Bytes.toBytes(filecontentHashes.size()));
			put.add(FAMILY, COLUMN_VALUES_FILES, 0l, hashSerializer.serialize(filecontentHashes));
			put.add(FAMILY, COLUMN_COUNT_PROJECTS, 0l, Bytes.toBytes(projnameHashes.size()));
			put.add(FAMILY, COLUMN_VALUES_PROJECTS, 0l, hashSerializer.serialize(projnameHashes));
			put.add(FAMILY, COLUMN_COUNT_VERSIONS, 0l, Bytes.toBytes(versionHashes.size()));
			put.add(FAMILY, COLUMN_VALUES_VERSIONS, 0l, hashSerializer.serialize(versionHashes));
			put.setWriteToWAL(false); // XXX massive performance increase!!!
			context.write(null, put);
		}

		private Result getRow(byte[] functionHash) throws IOException {
			Get get = new Get(functionHash);
			get.addFamily(FAMILY);
			return this.indexFunctions2Files.get(get);
		}
	}

	@Override
	public void run() {
		try {
			HbaseWrapper.truncate(this.indexHashfacts2Functions);
			
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			HbaseWrapper.launchMapReduceJob(
					IndexHashfacts2FunctionsCreator.class.getName()+" Job", 
					"functions", 
					"indexHashfacts2Functions",
					scan,
					IndexHashfacts2FunctionsCreator.class, 
					IndexHashfacts2FunctionsMapper.class, 
					IndexHashfacts2FunctionsReducer.class, 
					ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);
			
			indexHashfacts2Functions.flushCommits();
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
