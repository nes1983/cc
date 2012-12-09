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
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import ch.unibe.scg.cc.activerecord.IPutFactory;
import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;
import ch.unibe.scg.cc.util.HashSerializer;
import ch.unibe.scg.cc.util.WrappedRuntimeException;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class IndexHashfacts2Functions implements Runnable {
	
	final HTable indexHashfacts2Functions;
	final HbaseWrapper hbaseWrapper;
	
	@Inject
	IndexHashfacts2Functions(@Named("indexHashfacts2Functions") HTable indexHashfacts2Functions, HbaseWrapper hbaseWrapper) {
		this.indexHashfacts2Functions = indexHashfacts2Functions;
		this.hbaseWrapper = hbaseWrapper;
	}
	
	public static class IndexHashfacts2FunctionsMapper<KEYOUT, VALUEOUT> extends GuiceTableMapper<KEYOUT, VALUEOUT> {
		@Override
		public void map(ImmutableBytesWritable uselessKey, Result value,
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
	
	public static class IndexHashfacts2FunctionsReducer extends GuiceTableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		
		final HTable indexFunctions2Files;
		final IPutFactory putFactory;
		final HashSerializer hashSerializer;
		final Provider<Set<byte[]>> byteSetProvider;
		
		@Inject
		public IndexHashfacts2FunctionsReducer(
				@Named("indexHashfacts2Functions") HTable indexHashfacts2Functions,
				@Named("indexFunctions2Files") HTable indexFunctions2Files,
				IPutFactory putFactory,
				HashSerializer hashSerializer,
				Provider<Set<byte[]>> byteSetProvider) {
			super(indexHashfacts2Functions);
			this.indexFunctions2Files = indexFunctions2Files;
			this.putFactory = putFactory;
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
				ImmutableBytesWritable value = i.next(); 
				byte[] functionHash = value.get();
				Result row = getRow(functionHash);
				byte[] vp = row.getValue(GuiceResource.FAMILY, GuiceResource.ColumnName.VALUES_VERSIONS.getName());
				byte[] vh = row.getValue(GuiceResource.FAMILY, GuiceResource.ColumnName.VALUES_FILES.getName());
				byte[] vf = row.getValue(GuiceResource.FAMILY, GuiceResource.ColumnName.VALUES_FUNCTIONS.getName());
				projnameHashes.addAll(hashSerializer.deserialize(vp, 20)); // XXX possible performance loss
				versionHashes.addAll(hashSerializer.deserialize(vh, 20)); // XXX possible performance loss
				filecontentHashes.addAll(hashSerializer.deserialize(vf, 20)); // XXX possible performance loss
				functionhashValues.addAll(hashSerializer.deserialize(functionHash, 20)); // XXX possible performance loss
			}
			
			Put put = putFactory.create(factHashKey.get());
			put.add(GuiceResource.FAMILY, GuiceResource.ColumnName.COUNT_FACTS.getName(), 0l, Bytes.toBytes(functionhashValues.size()));
			put.add(GuiceResource.FAMILY, GuiceResource.ColumnName.VALUES_FACTS.getName(), 0l, hashSerializer.serialize(functionhashValues));
			put.add(GuiceResource.FAMILY, GuiceResource.ColumnName.COUNT_FUNCTIONS.getName(), 0l, Bytes.toBytes(filecontentHashes.size()));
			put.add(GuiceResource.FAMILY, GuiceResource.ColumnName.VALUES_FUNCTIONS.getName(), 0l, hashSerializer.serialize(filecontentHashes));
			put.add(GuiceResource.FAMILY, GuiceResource.ColumnName.COUNT_VERSIONS.getName(), 0l, Bytes.toBytes(projnameHashes.size()));
			put.add(GuiceResource.FAMILY, GuiceResource.ColumnName.VALUES_VERSIONS.getName(), 0l, hashSerializer.serialize(projnameHashes));
			put.add(GuiceResource.FAMILY, GuiceResource.ColumnName.COUNT_FILES.getName(), 0l, Bytes.toBytes(versionHashes.size()));
			put.add(GuiceResource.FAMILY, GuiceResource.ColumnName.VALUES_FILES.getName(), 0l, hashSerializer.serialize(versionHashes));
			write(put);
		}

		private Result getRow(byte[] functionHash) throws IOException {
			Get get = new Get(functionHash);
			get.addFamily(GuiceResource.FAMILY);
			return indexFunctions2Files.get(get);
		}
	}

	@Override
	public void run() {
		try {
			hbaseWrapper.truncate(indexHashfacts2Functions);
			
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			hbaseWrapper.launchTableMapReduceJob(
					IndexHashfacts2Functions.class.getName()+" Job", 
					"functions", 
					"indexHashfacts2Functions",
					scan,
					IndexHashfacts2Functions.class, 
					"IndexHashfacts2FunctionsMapper", 
					"IndexHashfacts2FunctionsReducer", 
					ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);
			
			indexHashfacts2Functions.flushCommits();
		} catch (IOException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (InterruptedException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (ClassNotFoundException e) {
			throw new WrappedRuntimeException(e.getCause());
		}
	}
	
	public static class IndexHashfacts2FunctionsTest {
		private static final byte[] FAMILY = Bytes.toBytes("d");
		
		@Test
		public void testRowScan() throws IOException {
			if(true)
				return;
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
