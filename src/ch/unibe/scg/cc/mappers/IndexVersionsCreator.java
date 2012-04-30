package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

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
import org.junit.Test;

import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class IndexVersionsCreator implements Runnable {
	
	HTable indexVersions;
	
	@Inject
	IndexVersionsCreator(@Named("indexVersions") HTable indexVersions) {
		this.indexVersions = indexVersions;
	}

	public static class IndexVersionsMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		
		ImmutableBytesWritable empty = new ImmutableBytesWritable(new byte[] {});
		
		public void map(ImmutableBytesWritable uselessKey, Result value, Context context) throws IOException, InterruptedException {
			byte[] key = value.getRow();
			assert key.length == 60;
			byte[] indexKey = Bytes.add(Bytes.head(Bytes.tail(key, 40), 20), Bytes.head(key, 20), Bytes.tail(key, 20));
			context.write(new ImmutableBytesWritable(indexKey), empty);
		}
	}
	
	public static class IndexVersionsReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
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
			HbaseWrapper.truncate(this.indexVersions);
			
			Scan scan = new Scan();
			HbaseWrapper.launchMapReduceJob(
					IndexVersionsCreator.class.getName()+" Job", 
					"versions", 
					"indexVersions",
					scan,
					IndexVersionsCreator.class, 
					IndexVersionsMapper.class, 
					IndexVersionsReducer.class, 
					ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);
			
			indexVersions.flushCommits();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static class VersionIndexTest {
		@Test
		public void testIndex() throws IOException {
			Injector i = Guice.createInjector(new CCModule(), new JavaModule());
			HTable versions = i.getInstance(Key.get(HTable.class, Names.named("versions")));
			HTable indexVersions = i.getInstance(Key.get(HTable.class, Names.named("indexVersions")));
			
			Scan scan = new Scan();
			ResultScanner rsVersions = versions.getScanner(scan);
			Iterator<Result> ir = rsVersions.iterator();
			int rowsToTest = 50; // XXX 
			
			while(ir.hasNext() && rowsToTest > 0) {
				byte[] fileContentHash = Bytes.head(Bytes.tail(ir.next().getRow(), 40), 20);
				Scan s = new Scan(fileContentHash);
				
				ResultScanner rsIndexFiles = indexVersions.getScanner(s);
				Iterator<Result> iri = rsIndexFiles.iterator();
				Assert.assertTrue(iri.hasNext());
				byte[] fileContentHashIndexTable = Bytes.head(iri.next().getRow(), 20);
				Assert.assertTrue(Arrays.equals(fileContentHash, fileContentHashIndexTable));
				rowsToTest--;
			}
		}
	}
}
