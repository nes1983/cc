package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import junit.framework.Assert;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

import ch.unibe.scg.cc.Frontend;
import ch.unibe.scg.cc.Java;
import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.RealProjectFactory;
import ch.unibe.scg.cc.activerecord.RealVersionFactory;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.mappers.IndexFiles2VersionsCreator.IndexFiles2VersionsMapper;
import ch.unibe.scg.cc.mappers.IndexFiles2VersionsCreator.IndexFiles2VersionsReducer;
import ch.unibe.scg.cc.mappers.TablePopulator.CharsetDetector;
import ch.unibe.scg.cc.mappers.TablePopulator.GuicePopulateMapper;
import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class IndexFactToProjectCreator implements Runnable {

	final HTable indexFactToProject;

	@Inject
	public IndexFactToProjectCreator(@Named("indexFactToProject") HTable indexFactToProject) {
		super();
		this.indexFactToProject = indexFactToProject;
	}
	
	public static class IndexFactToProjectMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {
		
		final GuiceFactToProjectMapper gfm;
		
		public IndexFactToProjectMapper() {
			Injector injector = Guice.createInjector(new CCModule(), new JavaModule());
			gfm = injector.getInstance(GuiceFactToProjectMapper.class);
		}
		
		public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			gfm.map(key, value, context);
		}
	}
	
	public static class GuiceFactToProjectMapper {
		
		final HTable indexProjects, indexVersions, indexFiles, indexFunctions, strings;
		final static IntWritable one = new IntWritable(1);
		
		@Inject
		public GuiceFactToProjectMapper(
				@Named("indexProjects") HTable indexProjects,
				@Named("indexVersions") HTable indexVersions, 
				@Named("indexFiles") HTable indexFiles, 
				@Named("indexFunctions") HTable indexFunctions,
				@Named("strings") HTable strings) {
			super();
			this.indexProjects = indexProjects;
			this.indexVersions = indexVersions;
			this.indexFiles = indexFiles;
			this.indexFunctions = indexFunctions;
			this.strings = strings;
		}
		
		public void map(ImmutableBytesWritable uselessKey, Result value, Context context) throws IOException, InterruptedException {
			byte[] key = value.getRow();
			byte[] hashFact = Bytes.head(key, 20);
			byte[] functionHash = Bytes.tail(key, 20);
			PrefixFilter functionFilter = new PrefixFilter(functionHash);
			Scan scanFunction = new Scan();
			scanFunction.setFilter(functionFilter);
			Iterator<Result> fileIndexIterator = this.indexFiles.getScanner(scanFunction).iterator();
			byte[] fileKey;
			while(fileIndexIterator.hasNext()) {
				fileKey = fileIndexIterator.next().getRow();
				byte[] fileContentHash = Bytes.tail(fileKey, 20);
				PrefixFilter fileFilter = new PrefixFilter(fileContentHash);
				Scan scanFile = new Scan();
				scanFile.setFilter(fileFilter);
				Iterator<Result> versionIndexIterator = this.indexVersions.getScanner(scanFile).iterator();
				byte[] versionKey;
				while(versionIndexIterator.hasNext()) {
					versionKey = versionIndexIterator.next().getRow();
					byte[] versionHash = Bytes.tail(Bytes.head(versionKey, 40), 20);
					byte[] projectKey = this.indexProjects.getScanner(new Scan(versionHash)).next().getRow();
					byte[] projectNameHash = Bytes.tail(projectKey, 20);
					byte[] indexKey = Bytes.add(hashFact, projectNameHash);
					context.write(new ImmutableBytesWritable(indexKey), one);
				}
			}
		}
	}
	
	public static class IndexFactToProjectReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
		public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
            for (IntWritable val : values) {
                    sum += val.get();
            }
			Put put = new Put(key.get());
			put.add(Bytes.toBytes("d"), Bytes.toBytes("nb"), 0l, Bytes.toBytes(sum));
			context.write(key, put);
		}
	}

	@Override
	public void run() {
		try {
			HbaseWrapper.truncate(this.indexFactToProject);
			
			Scan scan = new Scan();
			HbaseWrapper.launchMapReduceJob(
					IndexFiles2VersionsCreator.class.getName()+" Job", 
					"indexFunctions", 
					"indexFactToProject",
					scan,
					IndexFactToProjectCreator.class, 
					IndexFactToProjectMapper.class, 
					IndexFactToProjectReducer.class, 
					ImmutableBytesWritable.class,
					IntWritable.class);
			
			indexFactToProject.flushCommits();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static class FactToProjectIndexTest {
		
		public static void main() throws IOException {
			Injector i = Guice.createInjector(new CCModule(), new JavaModule());
			HTable f2p = i.getInstance(Key.get(HTable.class, Names.named("indexFactToProject")));
			HTable strings = i.getInstance(Key.get(HTable.class, Names.named("strings")));
			
			Scan scan = new Scan();
			scan.setCaching(500000);
			ResultScanner rsF2p = f2p.getScanner(scan);
			Iterator<Result> ir = rsF2p.iterator();
			
			List<F2P> list = new ArrayList<IndexFactToProjectCreator.FactToProjectIndexTest.F2P>();
			while(ir.hasNext()) {
				Result r = ir.next();
				list.add(new F2P(Bytes.head(r.getRow(), 20), Bytes.tail(r.getRow(), 20), Bytes.toInt(r.getValue(Bytes.toBytes("d"), Bytes.toBytes("nb")))));
			}
			Collections.sort(list);
			
			for(F2P f : list) {
				if(f.getCount() > 130) {
					Result projname = strings.get(new Get(f.getProjectHash()));
					System.out.println("Hash " + Arrays.toString(f.getFactHash()) + " appears " + f.getCount() + " times in project "+Bytes.toString(projname.getValue(Bytes.toBytes("d"), Bytes.toBytes("pn")))+".");
				}
			}
		}
		
		public static class F2P implements Comparable<F2P> {
			byte[] factHash, projectHash;
			int count;
			
			F2P(byte[] factHash, byte[] projectHash, int count) {
				super();
				this.factHash = factHash;
				this.projectHash = projectHash;
				this.count = count;
			}

			@Override
			public int compareTo(F2P o) {
				if(o.getCount() == getCount())
					return 0;
				else if(o.getCount() > getCount())
					return 1;
				return -1;
			}

			public byte[] getFactHash() {
				return factHash;
			}

			public byte[] getProjectHash() {
				return projectHash;
			}

			private int getCount() {
				return this.count;
			}
		}
	}
}
