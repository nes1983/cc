package ch.unibe.scg.cc;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapreduce.Counter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.CodeFileFactory;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Function.FunctionFactory;
import ch.unibe.scg.cc.lines.StringOfLines;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;
import ch.unibe.scg.cc.mappers.Constants;
import ch.unibe.scg.cc.mappers.HTableWriteBuffer;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;

@SuppressWarnings("javadoc")
public class RegisterClonesBackendTest {
	final StringOfLinesFactory stringOfLinesFactory = new StringOfLinesFactory();
	final StringOfLines aThruF = stringOfLinesFactory.make("a\nb\nc\nd\ne\nf\n");
	final StringOfLines aThruK = stringOfLinesFactory.make("a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\n");

	HTableWriteBuffer f2f = mock(HTableWriteBuffer.class);
	HTableWriteBuffer strings = mock(HTableWriteBuffer.class);
	HTableWriteBuffer f2s = mock(HTableWriteBuffer.class);

	Backend backend;
	Injector i;
	Function fun;

	@Before
	public void setUp() {
		Module m = new AbstractModule() {
			@Override protected void configure() {
				bind(HTableWriteBuffer.class).annotatedWith(Names.named("file2function")).toInstance(f2f);
				bind(HTableWriteBuffer.class).annotatedWith(Names.named("strings")).toInstance(strings);
				bind(HTableWriteBuffer.class).annotatedWith(Names.named("function2snippet")).toInstance(f2s);

				bind(Counter.class).annotatedWith(Names.named(Constants.COUNTER_CANNOT_BE_HASHED)).toInstance(
						mock(Counter.class));
				bind(Counter.class).annotatedWith(Names.named(Constants.COUNTER_SUCCESSFULLY_HASHED)).toInstance(
						mock(Counter.class));
			}
		};
		i = Guice.createInjector(Modules.override(new CCModule()).with(m));
		backend = i.getInstance(Backend.RegisterClonesBackend.class);

		fun = i.getInstance(FunctionFactory.class).makeFunction(i.getInstance(StandardHasher.class), 0, "", "");
	}

	@Test
	public void testOneRegister() throws IOException {
		backend.registerConsecutiveLinesOfCode(aThruF, fun, Main.TYPE_3_CLONE);
		assertThat(fun.getSnippets().size(), is(2));
		assertThat(fun.getSnippets().get(0).getSnippet(), is("a\nb\nc\nd\ne\n"));
		assertThat(fun.getSnippets().get(1).getSnippet(), is("\nb\nc\nd\ne\nf\n"));


		CodeFile cf = i.getInstance(CodeFileFactory.class).create(aThruF.toString());
		cf.addFunction(fun);
		backend.register(cf);

		verify(f2f, Mockito.times(1)).write(Mockito.any(Put.class));
		verify(strings, Mockito.times(1)).write(Mockito.any(Put.class));
		verify(f2s, Mockito.times(2)).write(Mockito.any(Put.class));
	}

	@Test
	public void testMoreRegisters() throws IOException {
		backend.registerConsecutiveLinesOfCode(aThruK, fun, Main.TYPE_3_CLONE);
		assertThat(fun.getSnippets().size(), is(7));
		assertThat(fun.getSnippets().get(0).getSnippet(), is("a\nb\nc\nd\ne\n"));
		assertThat(fun.getSnippets().get(6).getSnippet(), is("\ng\nh\ni\nj\nk\n"));


		CodeFile cf = i.getInstance(CodeFileFactory.class).create(aThruK.toString());
		cf.addFunction(fun);
		backend.register(cf);

		verify(f2f, Mockito.times(1)).write(Mockito.any(Put.class));
		verify(strings, Mockito.times(1)).write(Mockito.any(Put.class));
		verify(f2s, Mockito.times(7)).write(Mockito.any(Put.class));
	}
}