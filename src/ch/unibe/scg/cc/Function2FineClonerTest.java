package ch.unibe.scg.cc;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.junit.Test;

import ch.unibe.scg.cc.Function2RoughClonerTest.CollectionCellSource;
import ch.unibe.scg.cc.Function2RoughClonerTest.TestModule;
import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.GitRepo;
import ch.unibe.scg.cc.javaFrontend.JavaModule;
import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.Codec;
import ch.unibe.scg.cells.Codecs;
import ch.unibe.scg.cells.InMemoryPipeline;
import ch.unibe.scg.cells.InMemoryShuffler;
import ch.unibe.scg.cells.InMemoryStorage;
import ch.unibe.scg.cells.Source;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Modules;

/** Test {@link Function2FineCloner}. */
public class Function2FineClonerTest {
	/** Test {@link Function2FineCloner#map}.*/
	@Test
	public void testMap() throws IOException, InterruptedException {
		Injector i = Guice.createInjector(Modules.override(new CCModule(new InMemoryStorage()),
				new JavaModule()).with(new TestModule()));
		Codec<GitRepo> repoCodec = i.getInstance(GitRepoCodec.class);
		CollectionCellSource<GitRepo> src = new CollectionCellSource<>(Arrays.<Iterable<Cell<GitRepo>>> asList(Arrays
				.asList(repoCodec.encode(GitPopulatorTest.parseZippedGit("paperExample.zip")))));

		ClonePipelineRunner runner = i.getInstance(ClonePipelineRunner.class);
		try (InMemoryShuffler<Clone> shuffler =
				i.getInstance(Key.get(new TypeLiteral<InMemoryShuffler<Clone>>() {}))) {
			InMemoryPipeline<GitRepo, Clone> pipe = InMemoryPipeline.make(src, shuffler);
			runner.run(pipe);
			shuffler.close();

			Source<Clone> rows = Codecs.decode(shuffler, i.getInstance(Function2RoughClonesCodec.class));

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
