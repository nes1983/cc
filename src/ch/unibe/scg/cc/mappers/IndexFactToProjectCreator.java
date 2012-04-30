package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import ch.unibe.scg.cc.Frontend;
import ch.unibe.scg.cc.Java;
import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.RealProjectFactory;
import ch.unibe.scg.cc.activerecord.RealVersionFactory;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.mappers.IndexVersionsCreator.IndexVersionsMapper;
import ch.unibe.scg.cc.mappers.IndexVersionsCreator.IndexVersionsReducer;
import ch.unibe.scg.cc.mappers.TablePopulator.CharsetDetector;
import ch.unibe.scg.cc.mappers.TablePopulator.GuicePopulateMapper;
import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class IndexFactToProjectCreator implements Runnable {

	final HTable indexFactToProject;

	@Inject
	public IndexFactToProjectCreator(@Named("indexFactToProject") HTable indexFactToProject) {
		super();
		this.indexFactToProject = indexFactToProject;
	}
	
	public static class IndexFactToProjectMapper extends TableMapper<ImmutableBytesWritable, IntWritable> {
		public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			Injector injector = Guice.createInjector(new CCModule(), new JavaModule());
			injector.getInstance(GuiceFactToProjectMapper.class).map(key, value, context);
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
			Iterator<Result> fileIndexIterator = this.indexFiles.getScanner(new Scan(functionHash)).iterator();
			byte[] fileKey;
			while(fileIndexIterator.hasNext() && Arrays.equals(Bytes.head((fileKey = fileIndexIterator.next().getRow()), 20), functionHash)) {
				byte[] fileContentHash = Bytes.tail(fileKey, 20);
				Iterator<Result> versionIndexIterator = this.indexVersions.getScanner(new Scan(fileContentHash)).iterator();
				byte[] versionKey;
				while(versionIndexIterator.hasNext() && Arrays.equals(Bytes.head((versionKey = versionIndexIterator.next().getRow()), 20), fileContentHash)) {
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
					IndexVersionsCreator.class.getName()+" Job", 
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
}
