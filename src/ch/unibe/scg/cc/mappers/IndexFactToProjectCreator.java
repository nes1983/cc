package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
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
import ch.unibe.scg.cc.mappers.TablePopulator.CharsetDetector;
import ch.unibe.scg.cc.mappers.TablePopulator.GuicePopulateMapper;
import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class IndexFactToProjectCreator {

	final HTable functions;

	@Inject
	public IndexFactToProjectCreator(@Named("functions") HTable functions) {
		this.functions = functions;
	}
	
	public static class IndexFactToProjectMapper extends TableMapper<Byte, Byte> {
		public void map(ImmutableBytesWritable key, Result value, Context context) {
			Injector injector = Guice.createInjector(new CCModule(), new JavaModule());
			injector.getInstance(GuiceIndexFactToProjectMapper.class).map(key, value, context);
		}
	}
	
	public static class IndexFactToProjectReducer extends TableReducer<Byte, Byte, Text> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) {
			
		}
	}
	
	public static class GuiceIndexFactToProjectMapper {
		@Inject
		GuiceIndexFactToProjectMapper(
				@Named("projects") HTable projects,
				@Named("versions") HTable versions,  
				@Named("files") HTable codefiles, 
				@Named("functions") HTable functions,
				@Named("strings") HTable strings) {
			super();
			this.projects = projects;
			this.versions = versions;
			this.codefiles = codefiles;
			this.functions = functions;
			this.strings = strings;
		}

		final HTable projects, versions,  codefiles, functions, strings;
		
		public void map(ImmutableBytesWritable key, Result value, Context context) {
			List<KeyValue> list = value.list();
			for(KeyValue kv : list) {
				byte[] functionHash = Bytes.head(kv.getKey(), 20);
				
//				Scan scan = new Scan();
//				scan.set
//				this.codefiles.getScanner().
//				
//				byte[] fileContentHash = Bytes.head(functionHash.getKey(), 20);
//				
//				byte[] versionHash;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Scan scan = new Scan();
		HbaseWrapper.launchMapReduceJob(
				"IndexFactToProjectCreatorJob", 
				"functions", 
				"indexFunctions", 
				scan,
				IndexFactToProjectCreator.class, 
				IndexFactToProjectMapper.class, 
				IndexFactToProjectReducer.class, 
				Text.class,
				IntWritable.class);
	}
}
