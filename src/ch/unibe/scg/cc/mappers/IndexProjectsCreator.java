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

public class IndexProjectsCreator implements Runnable {
	
	HTable indexProjects;
	
	@Inject
	IndexProjectsCreator(@Named("indexProjects") HTable indexProjects) {
		this.indexProjects = indexProjects;
	}

	public static class IndexProjectsMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		
		BytesWritable empty = new BytesWritable(new byte[] {});
		
		public void map(ImmutableBytesWritable uselessKey, Result value, Context context) throws IOException, InterruptedException {
			byte[] key = value.getRow();
			byte[] indexKey = Bytes.add(Bytes.tail(key, 20), Bytes.head(key, 20));
			byte[] versionNumber = value.getValue(Bytes.toBytes("d"), Bytes.toBytes("vn"));
			context.write(new ImmutableBytesWritable(indexKey), new ImmutableBytesWritable(versionNumber));
		}
	}
	
	public static class IndexProjectsReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
			Put put = new Put(key.get());
			put.add(Bytes.toBytes("d"), Bytes.toBytes("vn"), 0l, values.iterator().next().get());
			context.write(key, put);
		}
	}

	@Override
	public void run() {
		try {
			HbaseWrapper.truncate(this.indexProjects);
			
			Scan scan = new Scan();
			HbaseWrapper.launchMapReduceJob(
					IndexProjectsCreator.class.getName()+" Job", 
					"projects", 
					"indexProjects",
					scan,
					IndexProjectsCreator.class, 
					IndexProjectsMapper.class, 
					IndexProjectsReducer.class, 
					ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);
			
			indexProjects.flushCommits();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static class ProjectIndexTest {
		@Test
		public void testIndex() throws IOException {
			Injector i = Guice.createInjector(new CCModule(), new JavaModule());
			HTable projects = i.getInstance(Key.get(HTable.class, Names.named("projects")));
			HTable indexProjects = i.getInstance(Key.get(HTable.class, Names.named("indexProjects")));
			
			Scan scan = new Scan();
			ResultScanner rsProjects = projects.getScanner(scan);
			Iterator<Result> ir = rsProjects.iterator();
			int rowsToTest = 50; // XXX 
			
			while(ir.hasNext() && rowsToTest > 0) {
				byte[] versionHash = Bytes.tail(ir.next().getRow(), 20);
				Scan s = new Scan(versionHash);
				
				ResultScanner rsindexProjects = indexProjects.getScanner(s);
				Iterator<Result> iri = rsindexProjects.iterator();
				Assert.assertTrue(iri.hasNext());
				byte[] versionHashIndexTable = Bytes.head(iri.next().getRow(), 20);
				Assert.assertTrue(Arrays.equals(versionHash, versionHashIndexTable));
				rowsToTest--;
			}
		}
	}
}
