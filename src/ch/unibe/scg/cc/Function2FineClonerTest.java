package ch.unibe.scg.cc;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Arrays;

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
import ch.unibe.scg.cells.Source;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Modules;

/** Test {@link Function2FineCloner}. */
public class Function2FineClonerTest {
	/** Test {@link Function2FineCloner#map}. */
	@Test
	public void testMap() throws IOException {
		Injector i = Guice.createInjector(Modules.override(new CCModule(), new JavaModule(), new InMemoryModule())
				.with(new TestModule()));
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

			assertThat(rows.toString(), is("[[thisSnippet {\n" +
					"  function: \"\\325\\'\\365A\\244\\317\\220\\232\\257\\361\\024(\\032\\313C\\260<n\\253\\251\"\n" +
					"  position: 5\n" +
					"  length: 14\n" +
					"}\n" +
					"thatSnippet {\n" +
					"  function: \"\\326\\030\\3332,\\363\\333.\\266n\\214#x\\332_\\315\\021\\001l\\332\"\n" +
					"  position: 3\n" +
					"  length: 15\n" +
					"}\n" +
					", thisSnippet {\n" +
					"  function: \"\\325\\'\\365A\\244\\317\\220\\232\\257\\361\\024(\\032\\313C\\260<n\\253\\251\"\n" +
					"  position: 13\n" +
					"  length: 6\n" +
					"}\n" +
					"thatSnippet {\n" +
					"  function: \"*\\277\\025\\333\\022C\\000\\273b\\302\\303\\273\\016X\\\"\\365\\304.\\205u\"\n" +
					"  position: 9\n" +
					"  length: 6\n" +
					"}\n" +
					"], [thisSnippet {\n" +
					"  function: \"\\326\\030\\3332,\\363\\333.\\266n\\214#x\\332_\\315\\021\\001l\\332\"\n" +
					"  position: 0\n" +
					"  length: 23\n" +
					"}\n" +
					"thatSnippet {\n" +
					"  function: \"*\\277\\025\\333\\022C\\000\\273b\\302\\303\\273\\016X\\\"\\365\\304.\\205u\"\n" +
					"  position: 0\n" +
					"  length: 20\n" +
					"}\n" +
					"]]"));
		}
	}
}
