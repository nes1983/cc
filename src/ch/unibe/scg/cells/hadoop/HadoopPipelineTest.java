package ch.unibe.scg.cells.hadoop;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.inject.Qualifier;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;

import ch.unibe.scg.cc.mappers.ConfigurationProvider;
import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.CellsModule;
import ch.unibe.scg.cells.Codec;
import ch.unibe.scg.cells.InMemoryStorage;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.protobuf.ByteString;

@SuppressWarnings("javadoc")
public final class HadoopPipelineTest {
	private final static String TABLE_NAME_IN = "richard-III";
	private final static String TABLE_NAME_EFF = "richard-wordcount";
	private Table<String> in;
	private Table<Long> eff;

	@Before
	public void loadRichardIII() throws IOException {
		String richard = CharStreams.toString(
				new InputStreamReader(this.getClass().getResourceAsStream("richard-iii.txt"), Charsets.UTF_8));
		richard.split("\\bACT\\s[IVX]+"); // TODO: bulk load table.
		TableAdmin tableAdmin = makeInjector().getInstance(TableAdmin.class);
		in = tableAdmin.createTable(TABLE_NAME_IN);
		eff = tableAdmin.createTable(TABLE_NAME_EFF);
	}

	@After
	public void deleteRichardIII() throws Exception {
		try(Closer c = Closer.create()) {
			c.register(in);
			c.register(eff);
		}

		TableAdmin tableAdmin = makeInjector().getInstance(TableAdmin.class);
		tableAdmin.deleteTable(in.getTableName());
		tableAdmin.deleteTable(eff.getTableName());
	}

	@Qualifier
	@Target({ FIELD, PARAMETER, METHOD })
	@Retention(RUNTIME)
	public static @interface In {}

	@Qualifier
	@Target({ FIELD, PARAMETER, METHOD })
	@Retention(RUNTIME)
	public static @interface Eff {}

	private class Act {
		final int number;
		final String content;

		Act(int number, String content) {
			this.number = number;
			this.content = content;
		}
	}

	private class RichardCodec implements Codec<Act> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<Act> encode(Act act) {
			return Cell.make(
					ByteString.copyFrom(Ints.toByteArray(act.number)),
					ByteString.copyFromUtf8("t"),
					ByteString.copyFromUtf8(act.content));
		}

		@Override
		public Act decode(Cell<Act> encoded) throws IOException {
			return new Act(
					Ints.fromByteArray(encoded.getRowKey().toByteArray()),
					encoded.getCellContents().toStringUtf8());
		}
	}

	private class WordCount {
		final String word;
		final int count;

		WordCount(String word, int count) {
			this.word = word;
			this.count = count;
		}
	}

	private class WordCountCodec implements Codec<WordCount> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<WordCount> encode(WordCount wc) {
			return Cell.make(
					ByteString.copyFromUtf8(wc.word),
					ByteString.copyFromUtf8("t"),
					ByteString.copyFrom(Ints.toByteArray(wc.count)));
		}

		@Override
		public WordCount decode(Cell<WordCount> encoded) throws IOException {
			return new WordCount(
					encoded.getRowKey().toStringUtf8(),
					Ints.fromByteArray(encoded.getCellContents().toByteArray()));
		}
	}

	private Injector makeInjector() {
		return Guice.createInjector(new HadoopModule(), new AbstractModule() {
			@Override protected void configure() {
				bind(Configuration.class).toProvider(ConfigurationProvider.class);
			}
		}, new CellsModule() {
			@Override protected void configure() {
				installTable(TABLE_NAME_IN, ByteString.EMPTY, In.class, new TypeLiteral<Act>() {},
						RichardCodec.class, new InMemoryStorage());
				installTable(TABLE_NAME_EFF, ByteString.EMPTY, Eff.class, new TypeLiteral<WordCount>() {},
						WordCountCodec.class, new InMemoryStorage());
			}
		});
	}

	// TODO: Add test.
}
