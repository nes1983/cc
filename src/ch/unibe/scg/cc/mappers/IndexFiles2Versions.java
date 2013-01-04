package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

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
import ch.unibe.scg.cc.util.HashSerializer;
import ch.unibe.scg.cc.util.WrappedRuntimeException;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class IndexFiles2Versions implements Runnable {

	final HTable indexFiles2Versions;
	final HBaseWrapper hbaseWrapper;

	@Inject
	IndexFiles2Versions(
			@Named("indexFiles2Versions") HTable indexFiles2Versions,
			HBaseWrapper hbaseWrapper) {
		this.indexFiles2Versions = indexFiles2Versions;
		this.hbaseWrapper = hbaseWrapper;
	}

	public static class IndexFiles2VersionsMapper<KEYOUT, VALUEOUT> extends
			GuiceTableMapper<KEYOUT, VALUEOUT> {
		@Override
		public void map(ImmutableBytesWritable uselessKey, Result value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			byte[] key = value.getRow();
			assert key.length == 60;
			byte[] fileContentHash = Bytes.head(Bytes.tail(key, 40), 20);
			byte[] versionHash = Bytes.head(key, 20);
			byte[] filePathHash = Bytes.tail(key, 20);
			byte[] restOfKey = Bytes.add(versionHash, filePathHash);
			context.write(new ImmutableBytesWritable(fileContentHash),
					new ImmutableBytesWritable(restOfKey));
		}
	}

	public static class IndexFiles2VersionsReducer
			extends
			GuiceTableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		final HTable indexVersions2Projects;
		final IPutFactory putFactory;
		final HashSerializer hashSerializer;
		final Provider<Set<byte[]>> byteSetProvider;

		@Inject
		public IndexFiles2VersionsReducer(
				@Named("indexFiles2Versions") HTable indexFiles2Versions,
				@Named("indexVersions2Projects") HTable indexVersions2Projects,
				IPutFactory putFactory, HashSerializer hashSerializer,
				Provider<Set<byte[]>> byteSetProvider) {
			super(indexFiles2Versions);
			this.indexVersions2Projects = indexVersions2Projects;
			this.putFactory = putFactory;
			this.hashSerializer = hashSerializer;
			this.byteSetProvider = byteSetProvider;
		}

		@Override
		public void reduce(ImmutableBytesWritable key,
				Iterable<ImmutableBytesWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<ImmutableBytesWritable> i = values.iterator();
			byte[] versionhashValues = new byte[] {};
			byte[] projecthashValues = new byte[] {};
			long versionsCounter = 0;
			long projectsCounter = 0;
			while (i.hasNext()) {
				ImmutableBytesWritable value = i.next();
				byte[] currentValue = value.get();
				byte[] versionHash = Bytes.head(currentValue, 20);
				byte[] projnameHash = getProjnameHash(versionHash);
				versionhashValues = Bytes.add(versionhashValues, versionHash);
				projecthashValues = Bytes.add(projecthashValues, projnameHash);
				versionsCounter++;
				projectsCounter++;
			}
			Put put = new Put(((ImmutableBytesWritable) key).get());
			put.add(GuiceResource.FAMILY,
					GuiceResource.ColumnName.COUNT_FILES.getName(), 0l,
					Bytes.toBytes(versionsCounter));
			put.add(GuiceResource.FAMILY,
					GuiceResource.ColumnName.COUNT_VERSIONS.getName(), 0l,
					Bytes.toBytes(projectsCounter));
			put.add(GuiceResource.FAMILY,
					GuiceResource.ColumnName.VALUES_FILES.getName(), 0l,
					versionhashValues);
			put.add(GuiceResource.FAMILY,
					GuiceResource.ColumnName.VALUES_VERSIONS.getName(), 0l,
					projecthashValues);
			context.write(null, put);
		}

		private byte[] getProjnameHash(byte[] versionHash) throws IOException {
			Iterator<Result> i = indexVersions2Projects.getScanner(
					new Scan(versionHash)).iterator();

			byte[] projnameHash = Bytes.tail(i.next().getRow(), 20);
			assert !i.hasNext();

			return projnameHash;
		}
	}

	@Override
	public void run() {
		try {
			hbaseWrapper.truncate(indexFiles2Versions);

			Scan scan = new Scan();
			hbaseWrapper.launchTableMapReduceJob(
					IndexFiles2Versions.class.getName() + " Job", "versions",
					"indexFiles2Versions", scan, IndexFiles2Versions.class,
					"IndexFiles2VersionsMapper", "IndexFiles2VersionsReducer",
					ImmutableBytesWritable.class, ImmutableBytesWritable.class,
					"-Xmx2000m");

			indexFiles2Versions.flushCommits();
		} catch (IOException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (InterruptedException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (ClassNotFoundException e) {
			throw new WrappedRuntimeException(e.getCause());
		}
	}

	public static class IndexFiles2VersionsTest {
		@Test
		public void testIndex() throws IOException {
			if (true)
				return;
			Injector i = Guice.createInjector(new CCModule(), new JavaModule());
			HTable versions = i.getInstance(Key.get(HTable.class,
					Names.named("versions")));
			HTable indexVersions = i.getInstance(Key.get(HTable.class,
					Names.named("indexFiles2Versions")));

			Scan scan = new Scan();
			ResultScanner rsVersions = versions.getScanner(scan);
			Iterator<Result> ir = rsVersions.iterator();
			int rowsToTest = 50; // XXX

			while (ir.hasNext() && rowsToTest > 0) {
				byte[] fileContentHash = Bytes.head(
						Bytes.tail(ir.next().getRow(), 40), 20);
				Scan s = new Scan(fileContentHash);

				ResultScanner rsIndexFiles = indexVersions.getScanner(s);
				Iterator<Result> iri = rsIndexFiles.iterator();
				Assert.assertTrue(iri.hasNext());
				byte[] fileContentHashIndexTable = Bytes.head(iri.next()
						.getRow(), 20);
				Assert.assertTrue(Arrays.equals(fileContentHash,
						fileContentHashIndexTable));
				rowsToTest--;
			}
		}
	}
}
