package ch.unibe.scg.cells.benchmarks;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.Cells;
import ch.unibe.scg.cells.Codec;
import ch.unibe.scg.cells.InMemoryPipeline;
import ch.unibe.scg.cells.LocalExecutionModule;
import ch.unibe.scg.cells.Mapper;
import ch.unibe.scg.cells.OneShotIterable;
import ch.unibe.scg.cells.Pipeline;
import ch.unibe.scg.cells.Sink;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;
import com.google.common.primitives.Ints;
import com.google.inject.Guice;
import com.google.protobuf.ByteString;

/**
 * Benchmarks cells performance on a local machine with wordcount problem.
 * Input folder can be specified via command line argument.
 */
public class CellsInMemoryWordCountBenchmark {
	private final static int TIMES = 50;

	final static class WordCount {
		final String word;
		final String fileName;
		int count;

		WordCount(String word, String fileName, int count) {
			this.count = count;
			this.word = word;
			this.fileName = fileName;
		}

		@Override
		public String toString() {
			return word + ": " + count;
		}
	}

	final static class WordCountCodec implements Codec<WordCount> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<WordCount> encode(WordCount s) {
			return Cell.make(ByteString.copyFromUtf8(s.word),
					ByteString.copyFromUtf8(s.fileName),
					ByteString.copyFrom(Ints.toByteArray(s.count)));
		}

		@Override
		public WordCount decode(Cell<WordCount> encoded) throws IOException {
			return new WordCount(encoded.getRowKey().toStringUtf8(),
					encoded.getColumnKey().toStringUtf8(),
					Ints.fromByteArray(encoded.getCellContents().toByteArray()));
		}
	}

	final static class Book {
		final String fileName;
		final String content;

		Book(String fileName, String content) {
			this.fileName = fileName;
			this.content = content;
		}
	}

	final static class BookCodec implements Codec<Book> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<Book> encode(Book b) {
			return Cell.make(ByteString.copyFromUtf8(b.fileName),
					ByteString.copyFromUtf8(b.content), ByteString.EMPTY);
		}

		@Override
		public Book decode(Cell<Book> encoded) throws IOException {
			return new Book(encoded.getRowKey().toStringUtf8(), encoded.getColumnKey().toStringUtf8());
		}
	}

	final static class WordParser implements Mapper<Book, WordCount> {
		private static final long serialVersionUID = 1L;

		@Override
		public void close() throws IOException { }

		@Override
		public void map(Book first, OneShotIterable<Book> row, Sink<WordCount> sink)
				throws IOException, InterruptedException {
			Map<String, WordCount> dictionary = new HashMap<>();
			for (Book book : row) {
				for (String word: book.content.split("\\s+")) {
					if (!word.isEmpty()) {
						if (!dictionary.containsKey(word)) {
							dictionary.put(word, new WordCount(word, book.fileName, 0));
						}
						dictionary.get(word).count++;
					}
				}
			}

			for (WordCount wc : dictionary.values()) {
				sink.write(wc);
			}
		}
	}

	final static class WordCounter implements Mapper<WordCount, WordCount> {
		private static final long serialVersionUID = 1L;

		@Override
		public void close() throws IOException { }

		@Override
		public void map(WordCount first, OneShotIterable<WordCount> row, Sink<WordCount> sink)
				throws IOException, InterruptedException {
			int count = 0;

			for (WordCount wc : row) {
				count += wc.count;
			}
			sink.write(new WordCount(first.word, first.fileName, count));
		}
	}

	/**
	 * Runs a wordcount benchmark. You can specify input folder with first argument.
	 * The default input folder is "benchmarks/data"
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		String input = "benchmarks/data";
		if (args.length > 0) {
			input = args[0];
		}

		double[] timings = new double[TIMES];
		NumberFormat f = NumberFormat.getInstance();
		f.setMaximumFractionDigits(2);

		for (int i = 0; i < TIMES; i++) {
			long startTime = System.nanoTime();

			try (InMemoryPipeline<Book, WordCount> pipe
						= Guice.createInjector(new LocalExecutionModule()).getInstance(InMemoryPipeline.Builder.class)
								.make(Cells.shard(Cells.encode(readBooksFromDisk(input), new BookCodec())))) {

				run(pipe);

				int dummy = 0;
				for (Iterable<WordCount> wcs : Cells.decodeSource(pipe.lastEfflux(), new WordCountCodec())) {
					dummy += Iterables.size(wcs);
				}

				timings[i] = (System.nanoTime() - startTime) / 1_000_000_000.0;
				System.out.println(f.format(timings[i]));

				if (dummy == 0) {
					System.out.println();
				}
			}
		}

		System.out.println("--------------");
		System.out.println(String.format("median: %s", f.format(median(timings))));
		System.out.println(String.format("min: %s", f.format(min(timings))));

	}

	static void run(Pipeline<Book, WordCount> pipe) throws IOException, InterruptedException {
		pipe.influx(new BookCodec())
			.map(new WordParser())
			.shuffle(new WordCountCodec())
			.mapAndEfflux(new WordCounter(), new WordCountCodec());
	}

	private static Iterable<Book> readBooksFromDisk(String path) {
		final ImmutableList.Builder<Book> ret = ImmutableList.builder();
		for (File f : new File(path).listFiles()) {
			try {
				ret.add(new Book(f.getName(),
						CharStreams.toString(new InputStreamReader(new FileInputStream(f), Charsets.UTF_8))));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return ret.build();
	}

	private static double median(double[] d) {
		if (d == null || d.length == 0) {
			throw new IllegalArgumentException("Median of 0 elements is undefined.");
		}

		double[] copy = Arrays.copyOf(d, d.length);
		Arrays.sort(copy);
		return copy[copy.length / 2];
	}

	private static double min(double[] d) {
		if (d == null || d.length == 0) {
			throw new IllegalArgumentException("Min of 0 elements is undefined.");
		}

		double min = d[0];
		for (int i = 1; i < d.length; i++) {
			if (d[i] < min) {
				min = d[i];
			}
		}

		return min;
	}
}
