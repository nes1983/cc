package ch.unibe.scg.cells.benchmarks;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import ch.unibe.scg.cells.Cells;
import ch.unibe.scg.cells.benchmarks.CellsInMemoryWordCountBenchmark.Book;
import ch.unibe.scg.cells.benchmarks.CellsInMemoryWordCountBenchmark.WordCount;
import ch.unibe.scg.cells.benchmarks.CellsInMemoryWordCountBenchmark.WordCountCodec;
import ch.unibe.scg.cells.hadoop.HadoopPipeline;
import ch.unibe.scg.cells.hadoop.RawTextFileFormat;
import ch.unibe.scg.cells.hadoop.Table;
import ch.unibe.scg.cells.hadoop.TableAdmin;
import ch.unibe.scg.cells.hadoop.UnibeModule;

import com.google.common.collect.Iterables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.ByteString;

/**
 * Benchmarks cells performance in cluster with wordcount problem.
 * Input hdfs folder can be specified via command line argument.
 */
public class CellsHadoopWordCountBenchmark {
	/**
	 * Runs a wordcount benchmark in cluster. You can specify input folder with first argument.
	 * The default input folder is "hdfs://haddock.unibe.ch/tmp/books"
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		String input = "hdfs://haddock.unibe.ch/tmp/books";
		if (args.length > 0) {
			input = args[0];
		}
		Injector inj = Guice.createInjector(new UnibeModule());
		final ByteString family = ByteString.copyFromUtf8("f");

		try (Table<WordCount> tab = inj.getInstance(TableAdmin.class).createTemporaryTable(family)) {
			HadoopPipeline<Book, WordCount> pipe = HadoopPipeline.fromHDFSToTable(
					inj.getInstance(Configuration.class),
					RawTextFileFormat.class,
					new Path(input),
					tab);
			CellsInMemoryWordCountBenchmark.run(pipe);

			long total = 0;
			for (Iterable<WordCount> wcs : Cells.decodeSource(tab.asCellSource(), new WordCountCodec())) {
				total += Iterables.size(wcs);
			}

			System.out.println(String.format("Total wordcounts: %s", total));
		}
	}
}
