package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import junit.framework.Assert;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.junit.Test;

import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class IndexFunctionsCreator implements Runnable {
	
	HTable indexFunctions;
	
	@Inject
	IndexFunctionsCreator(@Named("indexFunctions") HTable indexFunctions) {
		this.indexFunctions = indexFunctions;
	}

	public static class IndexFunctionsMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		
		ImmutableBytesWritable empty = new ImmutableBytesWritable(new byte[] {});
		
		public void map(ImmutableBytesWritable uselessKey, Result value, Context context) throws IOException, InterruptedException {
			byte[] key = value.getRow();
			byte[] indexKey = Bytes.add(Bytes.tail(key, 20), Bytes.head(Bytes.tail(key, 21), 1), Bytes.head(key, 20));
			
			context.write(new ImmutableBytesWritable(indexKey), empty);
		}
	}
	
	public static class IndexFunctionsReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
			Put put = new Put(key.get());
			byte[] dummy = values.iterator().next().get();
			put.add(Bytes.toBytes("d"), dummy, 0l, dummy);
			context.write(key, put);
		}
	}

	@Override
	public void run() {
		try {
			HbaseWrapper.truncate(this.indexFunctions);
			
			Scan scan = new Scan();
			HbaseWrapper.launchMapReduceJob(
					IndexFunctionsCreator.class.getName()+" Job", 
					"functions", 
					"indexFunctions",
					scan,
					IndexFunctionsCreator.class, 
					IndexFunctionsMapper.class, 
					IndexFunctionsReducer.class, 
					ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);
			
			indexFunctions.flushCommits();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static class FunctionIndexTest {
		@Test
		public void testIndex() throws IOException {
			Injector i = Guice.createInjector(new CCModule(), new JavaModule());
			HTable functions = i.getInstance(Key.get(HTable.class, Names.named("functions")));
			HTable indexFunctions = i.getInstance(Key.get(HTable.class, Names.named("indexFunctions")));
			
			Scan scan = new Scan();
			ResultScanner rsFunctions = functions.getScanner(scan);
			Iterator<Result> ir = rsFunctions.iterator();
			int rowsToTest = 100; // XXX 
			
			while(ir.hasNext() && rowsToTest > 0) {
				byte[] factHash = Bytes.tail(ir.next().getRow(), 20);
				Scan s = new Scan(factHash);
				
				ResultScanner rsIndexFunctions = indexFunctions.getScanner(s);
				Iterator<Result> iri = rsIndexFunctions.iterator();
				Assert.assertTrue(iri.hasNext());
				byte[] factHashIndexTable = Bytes.head(iri.next().getRow(), 20);
				Assert.assertTrue(Arrays.equals(factHash, factHashIndexTable));
				rowsToTest--;
			}
		}

		@Test
		public void testNotNullIndex() throws IOException {
			Injector i = Guice.createInjector(new CCModule(), new JavaModule());
			HTable indexFunctions = i.getInstance(Key.get(HTable.class, Names.named("indexFunctions")));
			
			byte[] empty = new byte[] {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
			Scan scan = new Scan();
			ResultScanner rsIndexFunctions = indexFunctions.getScanner(scan);
			Iterator<Result> iri = rsIndexFunctions.iterator();
			byte[] factHashIndexTable = Bytes.head(iri.next().getRow(), 20);
			Assert.assertFalse(Arrays.equals(empty, factHashIndexTable));
		}
	}
}
