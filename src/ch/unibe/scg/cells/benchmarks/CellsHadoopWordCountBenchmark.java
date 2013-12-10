package ch.unibe.scg.cells.benchmarks;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;

import ch.unibe.scg.cells.benchmarks.CellsInMemoryWordCountBenchmark.FileContent;
import ch.unibe.scg.cells.benchmarks.CellsInMemoryWordCountBenchmark.WordCount;
import ch.unibe.scg.cells.hadoop.HadoopPipeline;
import ch.unibe.scg.cells.hadoop.Table;
import ch.unibe.scg.cells.hadoop.TableAdmin;
import ch.unibe.scg.cells.hadoop.UnibeModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.ByteString;

/**
 * Benchmarks cells performance in cluster with wordcount problem.
 * Input hdfs folder can be specified via command line argument.
 */
public final class CellsHadoopWordCountBenchmark {
	/**
	 * Runs a wordcount benchmark in cluster. You can specify input folder with first argument.
	 * The default input folder is "hdfs://haddock.unibe.ch/tmp/books"
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		String input = HadoopBenchmark.INPUT_PATH;
		if (args.length > 0) {
			input = args[0];
		}
		Injector inj = Guice.createInjector(new UnibeModule());
		final ByteString family = ByteString.copyFromUtf8("f");

		Configuration c = inj.getInstance(Configuration.class);
		c.setLong(MRJobConfig.MAP_MEMORY_MB, 1400L);
		c.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx1100m");
		c.setLong(MRJobConfig.REDUCE_MEMORY_MB, 1400L);
		c.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx1100m");

		try (Table<WordCount> tab = inj.getInstance(TableAdmin.class).createTemporaryTable(family)) {
			HadoopPipeline<FileContent, WordCount> pipe = HadoopPipeline.fromHDFSToTable(
					inj.getInstance(Configuration.class),
					RawFileFormat.class,
					new Path(input),
					tab);
			CellsInMemoryWordCountBenchmark.run(pipe);
		}
	}
}
