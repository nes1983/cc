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
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class IndexFiles2VersionsCreator implements Runnable {
	
	HTable indexFiles2Versions;
	
	@Inject
	IndexFiles2VersionsCreator(@Named("indexFiles2Versions") HTable indexFiles2Versions) {
		this.indexFiles2Versions = indexFiles2Versions;
	}

	public static class IndexFiles2VersionsMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		public void map(ImmutableBytesWritable uselessKey, Result value, Context context) throws IOException, InterruptedException {
			byte[] key = value.getRow();
			assert key.length == 60;
			byte[] fileContentHash = Bytes.head(Bytes.tail(key, 40), 20);
			byte[] versionHash = Bytes.head(key, 20);
			byte[] filePathHash = Bytes.tail(key, 20);
			byte[] restOfKey = Bytes.add(versionHash, filePathHash);
			context.write(new ImmutableBytesWritable(fileContentHash), new ImmutableBytesWritable(restOfKey));
		}
	}
	
	public static class IndexFiles2VersionsReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		final GuiceIndexFiles2VersionsReducer g;
		
		public IndexFiles2VersionsReducer() {
			Injector injector = Guice.createInjector(new CCModule(), new JavaModule());
			g = injector.getInstance(GuiceIndexFiles2VersionsReducer.class);
		}
		
		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
			g.reduce(key, values, context);
		}
	}
	
	public static class GuiceIndexFiles2VersionsReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		private static final byte[] FAMILY = Bytes.toBytes("d");
		private static final byte[] COLUMN_COUNT_VERSIONS = Bytes.toBytes("nv");
		private static final byte[] COLUMN_COUNT_PROJECTS = Bytes.toBytes("np");
		private static final byte[] COLUMN_VALUES_VERSIONS = Bytes.toBytes("vv");
		private static final byte[] COLUMN_VALUES_PROJECTS = Bytes.toBytes("vp");
		private final HTable indexProjects;
		
		@Inject
		public GuiceIndexFiles2VersionsReducer(@Named("indexProjects") HTable indexProjects) {
			this.indexProjects = indexProjects;
		}

		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
			Iterator<ImmutableBytesWritable> i = values.iterator();
			byte[] versionhashValues = new byte[] {};
			byte[] projecthashValues = new byte[] {};
			long versionsCounter = 0;
			long projectsCounter = 0;
			while(i.hasNext()) {
				byte[] currentValue = i.next().get();
				byte[] versionHash = Bytes.head(currentValue, 20);
				byte[] projnameHash = getProjnameHash(versionHash);
				versionhashValues = Bytes.add(versionhashValues, versionHash);
				projecthashValues = Bytes.add(projecthashValues, projnameHash);
				versionsCounter++;
				projectsCounter++;
			}
			Put put = new Put(key.get());
			put.add(FAMILY, COLUMN_COUNT_VERSIONS, 0l, Bytes.toBytes(versionsCounter));
			put.add(FAMILY, COLUMN_COUNT_PROJECTS, 0l, Bytes.toBytes(projectsCounter));
			put.add(FAMILY, COLUMN_VALUES_VERSIONS, 0l, versionhashValues);
			put.add(FAMILY, COLUMN_VALUES_PROJECTS, 0l, projecthashValues);
			context.write(key, put);
		}

		private byte[] getProjnameHash(byte[] versionHash) throws IOException {
			Iterator<Result> i = indexProjects.getScanner(new Scan(versionHash)).iterator();
			
			byte[] projnameHash = Bytes.tail(i.next().getRow(), 20);
			assert !i.hasNext();
			
			return projnameHash;
		}
	}

	@Override
	public void run() {
		try {
			HbaseWrapper.truncate(this.indexFiles2Versions);
			
			Scan scan = new Scan();
			HbaseWrapper.launchMapReduceJob(
					IndexFiles2VersionsCreator.class.getName()+" Job", 
					"versions", 
					"indexFiles2Versions",
					scan,
					IndexFiles2VersionsCreator.class, 
					IndexFiles2VersionsMapper.class, 
					IndexFiles2VersionsReducer.class, 
					ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);
			
			indexFiles2Versions.flushCommits();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static class IndexFiles2VersionsTest {
		@Test
		public void testIndex() throws IOException {
			Injector i = Guice.createInjector(new CCModule(), new JavaModule());
			HTable versions = i.getInstance(Key.get(HTable.class, Names.named("versions")));
			HTable indexVersions = i.getInstance(Key.get(HTable.class, Names.named("indexFiles2Versions")));
			
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
