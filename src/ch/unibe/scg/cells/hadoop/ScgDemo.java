package ch.unibe.scg.cells.hadoop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;

import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.CellsModule;
import ch.unibe.scg.cells.Codec;
import ch.unibe.scg.cells.Codecs;
import ch.unibe.scg.cells.InMemoryPipeline;
import ch.unibe.scg.cells.InMemoryShuffler;
import ch.unibe.scg.cells.Mapper;
import ch.unibe.scg.cells.OneShotIterable;
import ch.unibe.scg.cells.Pipeline;
import ch.unibe.scg.cells.Sink;
import ch.unibe.scg.cells.hadoop.HadoopPipelineTest.Act;
import ch.unibe.scg.cells.hadoop.HadoopPipelineTest.ActCodec;
import ch.unibe.scg.cells.hadoop.HadoopPipelineTest.In;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.protobuf.ByteString;

@SuppressWarnings("javadoc")
public final class ScgDemo {
	/** Not part of the demo. It's just how the data gets there beforehand. */
	@Test
	@Ignore
	public void createRichard() throws IOException, InterruptedException  {
		final String tableName = "richard-iii";
		final ByteString family = ByteString.copyFromUtf8("f");
		Module tab = new CellsModule() {
			@Override protected void configure() {
				installTable(
						In.class,
						new TypeLiteral<Act>() {},
						ActCodec.class,
						new HBaseStorage(), new HBaseTableModule<>(tableName, family));
			}
		};
		Injector i = Guice.createInjector(new UnibeModule(), tab);
		i.getInstance(TableAdmin.class).createTable("richard-iii", family);
		try (Sink<Act> s = i.getInstance(Key.get(new TypeLiteral<Sink<Act>>() {}, In.class))) {
			for (Act act : HadoopPipelineTest.readActsFromDisk()) {
				s.write(act);
			}
		}
	}

	static class Word {
		final String word;
		final int act;
		final int pos;

		public Word(String word, int act, int pos) {
			this.word = word;
			this.act = act;
			this.pos = pos;
		}
	}

	static class WordCodec implements Codec<Word> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<Word> encode(Word s) {
			ByteBuffer col = ByteBuffer.allocate(2 * Ints.BYTES);
			col.putInt(s.act);
			col.putInt(s.pos);
			col.rewind();

			return Cell.make(ByteString.copyFromUtf8(s.word),
					ByteString.copyFrom(col),
					ByteString.EMPTY);
		}

		@Override
		public Word decode(Cell<Word> encoded) throws IOException {
			ByteBuffer col = encoded.getColumnKey().asReadOnlyByteBuffer();
			int act = col.getInt();
			int pos = col.getInt();
			return new Word(encoded.getRowKey().toStringUtf8(), act, pos);
		}
	}

	static class WordParser implements Mapper<Act, Word> {
		private static final long serialVersionUID = 1L;

		@Override
		public void close() throws IOException { }

		@Override
		public void map(Act first, OneShotIterable<Act> row, Sink<Word> sink)
				throws IOException, InterruptedException {
			for (Act act : row) {
				Matcher matcher = Pattern.compile("\\w+").matcher(act.content);
				while (matcher.find()) {
					sink.write(new Word(matcher.group(), first.number, matcher.start()));
				}
			}
		}
	}

	static class WordCounter implements Mapper<Word, WordCount> {
		private static final long serialVersionUID = 1L;

		@Override
		public void close() throws IOException { }

		@Override
		// TODO: think of better name for first?!
		public void map(Word first, OneShotIterable<Word> row, Sink<WordCount> sink) throws IOException,
				InterruptedException {
			long count = Iterables.size(row);
			sink.write(new WordCount(count, first.word));
		}
	}

	static class WordCount {
		final long count;
		final String word;

		WordCount(long count, String word) {
			this.count = count;
			this.word = word;
		}

		@Override
		public String toString() {
			return word + " " + count;
		}
	}

	static class WordCountCodec implements Codec<WordCount> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<WordCount> encode(WordCount s) {
			return Cell.make(ByteString.copyFromUtf8(s.word),
					ByteString.copyFrom(Longs.toByteArray(s.count)),
					ByteString.EMPTY);
		}

		@Override
		public WordCount decode(Cell<WordCount> encoded) throws IOException {
			return new WordCount(Longs.fromByteArray(encoded.getColumnKey().toByteArray()),
					encoded.getRowKey().toStringUtf8());
		}
	}

	@Test
	public void runInHadoop() throws IOException {
		Injector i = Guice.createInjector(new UnibeModule());
		TableAdmin tableAdmin = i.getInstance(TableAdmin.class);
		try (Table<Act> in = tableAdmin.existing("richard-iii", ByteString.copyFromUtf8("f"));
				Table<WordCount> eff = tableAdmin.existing("richard-wordcount", ByteString.copyFromUtf8("f"))) {
			HadoopPipeline.fromTableToTable(i.getInstance(Configuration.class), in, eff);
		}
	}

	void run(Pipeline<Act, WordCount> pipe) throws IOException, InterruptedException {
		pipe
			.influx(new ActCodec())
			.map(new WordParser())
			.shuffle(new WordCodec())
			.mapAndEfflux(new WordCounter(), new WordCountCodec());
	}

	@Test
	public void countWords() throws IOException, InterruptedException {
		Injector i = Guice.createInjector(new UnibeModule());
		TableAdmin tableAdmin = i.getInstance(TableAdmin.class);

		InMemoryShuffler<WordCount> eff = InMemoryShuffler.getInstance();
		try (Table<Act> in = tableAdmin.existing("richard-iii", ByteString.copyFromUtf8("f"))) {
			Pipeline<Act, WordCount> pipe = InMemoryPipeline.make(in.asCellSource(), eff);
			run(pipe);
		}

		for (Iterable<WordCount> wcs : Codecs.decode(eff, new WordCountCodec())) {
			for (WordCount wc : wcs) {
				System.out.println(wc);
			}
		}
	}
}
