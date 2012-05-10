package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class MRMain {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws Exception {
		assert args.length == 1;
		Class c = Class.forName(args[0]);
		Injector injector = Guice.createInjector(new CCModule(), new JavaModule());
		Object instance = injector.getInstance(c);
		((Runnable) instance).run();
	}
	
	public static class MRMainMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		GuiceMapper gm;
		
		@Override
		public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			if(gm == null) {
				String clazz = context.getConfiguration().get("GuiceMapperAnnotation");
				Injector injector = Guice.createInjector(new CCModule(), new JavaModule());
				gm = injector.getInstance(Key.get(GuiceMapper.class, Names.named(clazz)));
			}
			gm.map(key, value, context);
		}
	}
	
	public static class MRMainReducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		GuiceReducer gr;
		
		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
			if(gr == null) {
				String clazz = context.getConfiguration().get("GuiceReducerAnnotation");
				Injector injector = Guice.createInjector(new CCModule(), new JavaModule());
				gr = injector.getInstance(Key.get(GuiceReducer.class, Names.named(clazz)));
			}
			gr.reduce(key, values, context);
		}
	}
}
