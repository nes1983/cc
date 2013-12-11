package ch.unibe.scg.cc;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import ch.unibe.scg.cc.Function2RoughClonerTest.TestModule;
import ch.unibe.scg.cc.GitInputFormat.GitRepoCodec;
import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.GitRepo;
import ch.unibe.scg.cells.CellSource;
import ch.unibe.scg.cells.Cells;
import ch.unibe.scg.cells.Codec;
import ch.unibe.scg.cells.InMemoryPipeline;
import ch.unibe.scg.cells.InMemoryStorage;
import ch.unibe.scg.cells.LocalCounterModule;
import ch.unibe.scg.cells.LocalExecutionModule;
import ch.unibe.scg.cells.Source;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

/** Test {@link Function2FineCloner}. */
public final class Function2FineClonerTest {
	/** Test {@link Function2FineCloner#map}.*/
	@Test
	public void testMap() throws IOException, InterruptedException {
		Injector i = Guice.createInjector(
				Modules.override(new CCModule(new InMemoryStorage(), new LocalCounterModule()))
						.with(new TestModule()),
				new LocalExecutionModule());
		Codec<GitRepo> repoCodec = i.getInstance(GitRepoCodec.class);
		try (CellSource<GitRepo> src
				= Cells.shard(Cells.encode(
						Arrays.asList(GitPopulatorTest.parseZippedGit("paperExample.zip")),
						repoCodec));
				InMemoryPipeline<GitRepo, Clone> pipe
					= i.getInstance(InMemoryPipeline.Builder.class).make(src)) {
			pipe
				.influx(repoCodec)
				.map(i.getInstance(GitPopulator.class))
				.shuffle(i.getInstance(Snippet2FunctionsCodec.class))
				.map(i.getInstance(Function2RoughCloner.class))
				.shuffle(i.getInstance(Function2RoughClonesCodec.class))
				.mapAndEfflux(i.getInstance(Function2FineCloner.class),
						i.getInstance(Function2RoughClonesCodec.class));

			try(Source<Clone> rows = pipe.lastEfflux()) {
				assertTrue(rows.toString(), Pattern.matches("(?s)\\[\\[thisSnippet \\{\n" +
						"  function: \".*?\"\n" +
						"  position: 5\n" +
						"  length: 14\n" +
						"\\}\n" +
						"thatSnippet \\{\n" +
						"  function: \".*?\"\n" +
						"  position: 3\n" +
						"  length: 15\n" +
						"\\}\n" +
						", thisSnippet \\{\n" +
						"  function: \".*?\"\n" +
						"  position: 13\n" +
						"  length: 6\n" +
						"\\}\n" +
						"thatSnippet \\{\n" +
						"  function: \".*?\"\n" +
						"  position: 9\n" +
						"  length: 6\n" +
						"\\}\n" +
						"\\], \\[thisSnippet \\{\n" +
						"  function: \".*?\"\n" +
						"  position: 0\n" +
						"  length: 23\n" +
						"\\}\n" +
						"thatSnippet \\{\n" +
						"  function: \".*?\"\n" +
						"  position: 0\n" +
						"  length: 20\n" +
						"\\}\n" +
						"\\]\\]", rows.toString()));
			}
		}
	}

	/** Just check if you can serialize a pipeline runner, including all mappers and codecs. */
	@Test
	@Ignore // TODO: InMemoryTables should actually not serialize. Test currently broken. Needs fix.
	public void testSerialization() throws IOException, ClassNotFoundException {
		Injector i = Guice.createInjector(
				Modules.override(new CCModule(new InMemoryStorage(), new LocalCounterModule()),
						new LocalExecutionModule())
				.with(new Function2RoughClonerTest.TestModule()));
		ByteArrayOutputStream bOut = new ByteArrayOutputStream();
		try (ObjectOutputStream out = new ObjectOutputStream(bOut)) {
			PipelineRunner runnerWithAllMappers = i.getInstance(PipelineRunner.class);
			out.writeObject(runnerWithAllMappers);
		}
		Assert.assertTrue(new ObjectInputStream(new ByteArrayInputStream(bOut.toByteArray())).readObject()
				instanceof PipelineRunner);
	}
}
