package ch.unibe.scg.cells.benchmarks;

import java.io.IOException;
import java.text.NumberFormat;

import ch.unibe.scg.cells.Cells;
import ch.unibe.scg.cells.InMemoryPipeline;
import ch.unibe.scg.cells.LocalExecutionModule;
import ch.unibe.scg.cells.benchmarks.CellsInMemoryWordCountBenchmark.FileContent;
import ch.unibe.scg.cells.benchmarks.CellsInMemoryWordCountBenchmark.FileContentCodec;

import com.google.common.collect.Iterables;
import com.google.inject.Guice;


/**
 * A cells job for training a distributed svm in memory. Map phase distributes data to svms,
 * reduce phase trains a set of svms on an subset of data.
 */
public final class CellsInMemorySVMBenchmark {
	private final static int TIMES = 5;

	/** Launches cells job. you can specify input path as parameter.
	 * @throws IOException
	 * @throws InterruptedException */
	public static void main(String args[]) throws IOException, InterruptedException {
		String input = "benchmarks/svmdata";

		double[] timings = new double[TIMES];
		NumberFormat f = NumberFormat.getInstance();
		f.setMaximumFractionDigits(2);

		for (int i = 0; i < TIMES; i++) {
			long startTime = System.nanoTime();

			try (InMemoryPipeline<FileContent, String> pipe
						= Guice.createInjector(new LocalExecutionModule()).getInstance(InMemoryPipeline.Builder.class)
								.make(Cells.shard(Cells.encode(CellsInMemoryWordCountBenchmark.readFilesFromDisk(input),
										new FileContentCodec())))) {

				CellsHadoopSVMBenchmark.run(pipe);

				int dummy = 0;
				for (Iterable<String> wcs : pipe.lastEfflux()) {
					dummy += Iterables.size(wcs);
				}

				if (dummy == 0) {
					System.out.println();
				}

				timings[i] = (System.nanoTime() - startTime) / 1_000_000_000.0;
				System.out.println(f.format(timings[i]));
			}
		}

		System.out.println("--------------");
		System.out.println(String.format("min: %s", f.format(CellsInMemoryWordCountBenchmark.min(timings))));
	}
}
