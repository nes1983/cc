package ch.unibe.scg.cc.mappers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;

import javax.inject.Named;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import ch.unibe.scg.cc.WrappedRuntimeException;
import ch.unibe.scg.cc.activerecord.Function;

import com.google.common.base.Optional;
import com.google.inject.Inject;

public class CountLOC implements Runnable {
	final MRWrapper hbaseWrapper;

	@Inject
	CountLOC(MRWrapper hbaseWrapper) {
		this.hbaseWrapper = hbaseWrapper;
	}

	@Override
	public void run() {
		try {
			Scan scan = new Scan();
			// Gets all columns from the specified family/qualifier.
			scan.addColumn(Constants.FAMILY, Function.FUNCTION_SNIPPET);

			Configuration config = new Configuration();
			config.set(MRJobConfig.MAP_MEMORY_MB, "1500");
			config.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx1000m");
			config.set(MRJobConfig.REDUCE_MEMORY_MB, "1500");
			config.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx1000m");
			config.setClass(Job.OUTPUT_FORMAT_CLASS_ATTR, NullOutputFormat.class, OutputFormat.class);

			hbaseWrapper.launchMapReduceJob(CountLOC.class.getName() + " Job", config, Optional.of("strings"),
					Optional.<String> absent(), Optional.of(scan), CountLOCMapper.class.getName(),
					Optional.<String> absent(), NullWritable.class, NullWritable.class);
		} catch (IOException | ClassNotFoundException e) {
			throw new WrappedRuntimeException(e);
		} catch (InterruptedException e) {
			// exit thread
			return;
		}
	}

	private static int countLines(String str) {
		LineNumberReader lnr = new LineNumberReader(new StringReader(checkNotNull(str)));
		try {
			lnr.skip(Long.MAX_VALUE);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		// getLineNumber() returns the number of line terminators, therefore we
		// have to add one to get the correct number of lines.
		return lnr.getLineNumber() + 1;
	}

	public static class CountLOCMapper extends GuiceTableMapper<NullWritable, NullWritable> {
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
		public void map(ImmutableBytesWritable uselessKey, Result value,
				@SuppressWarnings("rawtypes") org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException,
				InterruptedException {
			String functionSnippet = Bytes.toString(value.getColumnLatest(Constants.FAMILY,
					Function.FUNCTION_SNIPPET).getValue());
			locCounter.increment(countLines(functionSnippet));
			functionsCounter.increment(1L);
		}
	}
}
