package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Iterator;

import javax.inject.Inject;
import javax.inject.Named;

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

import com.google.inject.Guice;
import com.google.inject.Injector;

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
		private final HTable indexFunctions2Files;
		
		@Inject
		public GuiceIndexHashfacts2FunctionsReducer(@Named("indexFunctions2Files") HTable indexFunctions2Files) {
			this.indexFunctions2Files = indexFunctions2Files;
		}

		@Override
		public void reduce(ImmutableBytesWritable factHashKey, Iterable<ImmutableBytesWritable> functionHashes, Context context) throws IOException, InterruptedException {
			Iterator<ImmutableBytesWritable> i = functionHashes.iterator();
			byte[] functionhashValues = new byte[] {};
			long functionCounter = 0;
			while(i.hasNext()) {
				byte[] currentValue = i.next().get();
				byte[] functionHash = currentValue;
				functionhashValues = Bytes.add(functionhashValues, functionHash);
				functionCounter++;
			}
			
			Put put = new Put(factHashKey.get());
			put.add(FAMILY, COLUMN_COUNT_FUNCTIONS, 0l, Bytes.toBytes(functionCounter));
			put.add(FAMILY, COLUMN_VALUES_FUNCTIONS, 0l, functionhashValues);
			context.write(factHashKey, put);
		}
	}

	@Override
	public void run() {
		try {
			HbaseWrapper.truncate(this.indexHashfacts2Functions);
			
			Scan scan = new Scan();
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
		
	}
}
