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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import ch.unibe.scg.cc.activerecord.IPutFactory;
import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;
import ch.unibe.scg.cc.util.WrappedRuntimeException;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class IndexVersions2Projects implements Runnable {
	
	final HTable indexVersions2Projects;
	
	@Inject
	IndexVersions2Projects(@Named("indexVersions2Projects") HTable indexVersions2Projects) {
		this.indexVersions2Projects = indexVersions2Projects;
	}
	
	public static class IndexVersions2ProjectsMapper<KEYOUT, VALUEOUT> extends GuiceTableMapper<KEYOUT, VALUEOUT> {
		@Override
		public void map(ImmutableBytesWritable uselessKey, Result value, org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
			byte[] key = value.getRow();
			byte[] indexKey = Bytes.add(Bytes.tail(key, 20), Bytes.head(key, 20));
			byte[] versionNumber = value.getValue(GuiceResource.FAMILY, Bytes.toBytes("vn"));
			context.write(new ImmutableBytesWritable(indexKey), new ImmutableBytesWritable(versionNumber));
		}
	}
	
	public static class IndexVersions2ProjectsReducer extends GuiceTableReducer<ImmutableBytesWritable,ImmutableBytesWritable,ImmutableBytesWritable> {

		private IPutFactory putFactory;

		@Inject
		IndexVersions2ProjectsReducer(
				@Named("indexVersions2Projects") HTable indexVersions2Projects,
				IPutFactory putFactory) {
			super(indexVersions2Projects);
			this.putFactory = putFactory;
		}

		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
			Put put = putFactory.create(key.get());
			put.add(GuiceResource.FAMILY, Bytes.toBytes("vn"), 0l, values.iterator().next().get());
			context.write(key, put);
		}
	}

	@Override
	public void run() {
		try {
			
			HbaseWrapper.truncate(indexVersions2Projects);
			
			Scan scan = new Scan();
			HbaseWrapper.launchTableMapReduceJob(
					IndexVersions2Projects.class.getName()+" Job", 
					"projects", 
					"indexVersions2Projects",
					scan,
					IndexVersions2Projects.class, 
					"IndexVersions2ProjectsMapper", 
					"IndexVersions2ProjectsReducer", 
					ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);
			
			indexVersions2Projects.flushCommits();
			
		} catch (IOException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (InterruptedException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (ClassNotFoundException e) {
			throw new WrappedRuntimeException(e.getCause());
		}
	}
	
	public static class ProjectIndexTest {
		@Test
		public void testIndex() throws IOException {
			if(true)
				return;
			Injector i = Guice.createInjector(new CCModule(), new JavaModule());
			HTable projects = i.getInstance(Key.get(HTable.class, Names.named("projects")));
			HTable indexProjects = i.getInstance(Key.get(HTable.class, Names.named("indexVersions2Projects")));
			
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
