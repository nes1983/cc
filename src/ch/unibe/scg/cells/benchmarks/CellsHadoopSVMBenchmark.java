package ch.unibe.scg.cells.benchmarks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;

import com.google.common.primitives.Ints;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.ByteString;

import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.Cells;
import ch.unibe.scg.cells.Codec;
import ch.unibe.scg.cells.Mapper;
import ch.unibe.scg.cells.OneShotIterable;
import ch.unibe.scg.cells.Pipeline;
import ch.unibe.scg.cells.Sink;
import ch.unibe.scg.cells.benchmarks.CellsInMemoryWordCountBenchmark.FileContent;
import ch.unibe.scg.cells.benchmarks.CellsInMemoryWordCountBenchmark.FileContentCodec;
import ch.unibe.scg.cells.benchmarks.SVM.TrainingInstance;
import ch.unibe.scg.cells.hadoop.HadoopPipeline;
import ch.unibe.scg.cells.hadoop.Table;
import ch.unibe.scg.cells.hadoop.TableAdmin;
import ch.unibe.scg.cells.hadoop.UnibeModule;

/**
 * A cells job for training a distributed svm. Map phase distributes data across machines,
 * reduce phase trains a set of svms on an subset of data.
 */
public final class CellsHadoopSVMBenchmark {
	static class StringCodec implements Codec<String> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<String> encode(String obj) {
			return Cell.make(ByteString.copyFromUtf8(obj), ByteString.copyFromUtf8("c"), ByteString.EMPTY);
		}

		@Override
		public String decode(Cell<String> cell) throws IOException {
			return cell.getRowKey().toStringUtf8();
		}
	}

	static class TrainingInstanceLine {
		final String line;
		final int shard;

		public TrainingInstanceLine(String line, int shard) {
			this.shard = shard;
			this.line = line;
		}
	}

	static class TrainingInstanceLineCodec implements Codec<TrainingInstanceLine> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<TrainingInstanceLine> encode(TrainingInstanceLine obj) {
			return Cell.make(ByteString.copyFrom(Ints.toByteArray(obj.shard)),
					ByteString.copyFromUtf8(obj.line), ByteString.EMPTY);
		}

		@Override
		public TrainingInstanceLine decode(Cell<TrainingInstanceLine> cell) throws IOException {
			return new TrainingInstanceLine(cell.getColumnKey().toStringUtf8(),
					Ints.fromByteArray(cell.getRowKey().toByteArray()));
		}
	}

	/** The class has to make sure that the data is shuffled to the various machines. */
	final static class DataDistributor implements Mapper<FileContent, TrainingInstanceLine> {
		private static final long serialVersionUID = 1L;
		static int outputs = 80;
		@Override
		public void close() throws IOException { }

		/** Spread the data around on  different machines. */
		@Override
		public void map(FileContent first, OneShotIterable<FileContent> row, Sink<TrainingInstanceLine> sink)
				throws IOException, InterruptedException {

			for (FileContent file : row) {
				try (Scanner sc = new Scanner(file.content)) {
					int i = 0;

					while (sc.hasNextLine()) {
						if (i == outputs) {
							i = 0;
						}

						sink.write(new TrainingInstanceLine(sc.nextLine(), i));
						i++;
					}
				}
			}
		}
	}

	/** Reducer outputs one line containing the hyperplane. */
	final static class SVMTrainer implements Mapper<TrainingInstanceLine, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void close() throws IOException { }

		/** Construct a hyperplane given the subset of training examples. */
		@Override
		public void map(TrainingInstanceLine first, OneShotIterable<TrainingInstanceLine> row, Sink<String> sink)
				throws IOException, InterruptedException {
			List<TrainingInstance> trainingSet = new ArrayList<>();

			for (TrainingInstanceLine line: row) {
				TrainingInstance instance = new TrainingInstance(line.line);
				trainingSet.add(instance);
			}

			SVM model = SVM.trainSVM(trainingSet, 10000);
			sink.write(model.toString());
		}
	}

	/** Launches cells job. you can specify input path as parameter. */
	public static void main(String[] args) throws IOException, InterruptedException {
		String input = HadoopSVMBenchmark.INPUT_PATH;
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
		c.setLong(MRJobConfig.NUM_REDUCES, DataDistributor.outputs);

		try (Table<String> tab = inj.getInstance(TableAdmin.class).createTemporaryTable(family)) {
			HadoopPipeline<FileContent, String> pipe = HadoopPipeline.fromHDFSToTable(
					c,
					RawFileFormat.class,
					new Path(input),
					tab);
			run(pipe);

			for (Iterable<String> lines : Cells.decodeSource(tab.asCellSource(), new StringCodec())) {
				for( String line : lines) {
					System.out.println(line);
				}
			}
		}
	}

	static void run(Pipeline<FileContent, String> pipe) throws IOException, InterruptedException {
		pipe
			.influx(new FileContentCodec())
			.map(new DataDistributor())
			.shuffle(new TrainingInstanceLineCodec())
			.mapAndEfflux(new SVMTrainer(), new StringCodec());
	}
}
