package ch.unibe.scg.cc.mappers;

import static ch.unibe.scg.cc.Utils.countLines;

import java.io.IOException;

import javax.inject.Named;
import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import ch.unibe.scg.cc.WrappedRuntimeException;

import com.google.common.base.Optional;
import com.google.inject.Inject;

/** Count the lines of code across all functions */
public class CountLOC implements Runnable {
	final MapReduceLauncher launcher;
	final Provider<Scan> scanProvider;

	@Inject
	CountLOC(MapReduceLauncher launcher, Provider<Scan> scanProvider) {
		this.launcher = launcher;
		this.scanProvider = scanProvider;
	}

	@Override
	public void run() {
		try {
			Scan scan = scanProvider.get();
			scan.addColumn(Constants.FAMILY, Function.FUNCTION_SNIPPET);

			Configuration config = new Configuration();
			config.set(MRJobConfig.MAP_MEMORY_MB, "1500");
			config.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx1000m");
			config.set(MRJobConfig.REDUCE_MEMORY_MB, "1500");
			config.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx1000m");
			config.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, NullOutputFormat.class, OutputFormat.class);

			launcher.launchMapReduceJob(CountLOC.class.getName() + " Job", config, Optional.of("strings"),
					Optional.<String> absent(), Optional.of(scan), CountLOCMapper.class.getName(),
					Optional.<String> absent(), NullWritable.class, NullWritable.class);
		} catch (IOException | ClassNotFoundException e) {
			throw new WrappedRuntimeException(e);
		} catch (InterruptedException e) {
			// exit thread
			return;
		}
	}

	static class CountLOCMapper extends GuiceTableMapper<NullWritable, NullWritable> {
		// Optional because in MRMain, we have an injector that does not set
		// this property, and can't, because it doesn't have the counter
		// available.
		@Inject(optional = true)
		@Named(Constants.COUNTER_FUNCTIONS)
		Counter functionsCounter;
		@Inject(optional = true)
		@Named(Constants.COUNTER_LOC)
		Counter locCounter;

		/** receives rows from htable strings */
		@Override
		public void map(ImmutableBytesWritable uselessKey, Result value, Context context) throws IOException,
				InterruptedException {
			String functionSnippet = Bytes.toString(value.getColumnLatest(Constants.FAMILY,
					Function.FUNCTION_SNIPPET).getValue());
			locCounter.increment(countLines(functionSnippet));
			functionsCounter.increment(1L);
		}
	}
}
