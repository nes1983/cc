package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.HBaseModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class MRMain extends Configured implements Tool {
	public static void main(String[] args) throws Throwable {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new MRMain(), args);
//		System.exit(res);
	}

	@Override
	public int run(String[] args) {
		System.out.println(Arrays.toString(args));
		assert args.length == 1;
		Class<?> c;
		try {
			c = Class.forName(args[0]);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		AbstractModule confModule = new AbstractModule() {
			@Override
			protected void configure() {
//				bind(Configuration.class).annotatedWith(Names.named("commandLine")).toInstance(getConf());
			}
		};
		Injector injector = Guice.createInjector(confModule, new CCModule(), new JavaModule(), new HBaseModule());
		Object instance = injector.getInstance(c);
		((Runnable) instance).run();
		return 0;
	}
	
	public static class MRMainMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
		GuiceMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> guiceMapper;
		
		@SuppressWarnings("unchecked")
		@Override
		public void setup(Context context) {
			String clazz = context.getConfiguration().get("GuiceMapperAnnotation");
			Injector injector = Guice.createInjector(new CCModule(), new JavaModule(), new HBaseModule());
			guiceMapper = injector.getInstance(Key.get(GuiceMapper.class, Names.named(clazz)));
		}
		
		@Override
		public void map(KEYIN key, VALUEIN value, Context context) throws IOException, InterruptedException {
			guiceMapper.map(key, value, context);
		}
	}
	
	public static class MRMainTableMapper<KEYOUT, VALUEOUT> extends TableMapper<KEYOUT, VALUEOUT> {
		GuiceTableMapper<KEYOUT, VALUEOUT> guiceMapper;
		
		@SuppressWarnings("unchecked")
		@Override
		public void setup(Context context) {
			String clazz = context.getConfiguration().get("GuiceMapperAnnotation");
			Injector injector = Guice.createInjector(new CCModule(), new JavaModule(), new HBaseModule());
			guiceMapper = injector.getInstance(Key.get(GuiceTableMapper.class, Names.named(clazz)));
		}
		
		@Override
		public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			guiceMapper.map(key, value, context);
		}
	}
	
	public static class MRMainTableReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		GuiceTableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> reducer;
		
		@SuppressWarnings("unchecked")
		@Override
		protected void setup(Context context) {
			String clazz = context.getConfiguration().get("GuiceReducerAnnotation");
			Injector injector = Guice.createInjector(new CCModule(), new JavaModule(), new HBaseModule());
			reducer = injector.getInstance(Key.get(GuiceTableReducer.class, Names.named(clazz)));
		}
		
		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
			reducer.reduce(key, values, context);
		}
	}
}
