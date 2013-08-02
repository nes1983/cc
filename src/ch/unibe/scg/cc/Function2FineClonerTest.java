package ch.unibe.scg.cc;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import ch.unibe.scg.cc.Function2RoughClonerTest.CollectionCellSource;
import ch.unibe.scg.cc.Function2RoughClonerTest.TestModule;
import ch.unibe.scg.cc.Protos.CloneGroup;
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
		try (InMemoryShuffler<CloneGroup> shuffler =
				i.getInstance(Key.get(new TypeLiteral<InMemoryShuffler<CloneGroup>>() {}))) {
			InMemoryPipeline<GitRepo, CloneGroup> pipe = InMemoryPipeline.make(src, shuffler);
			runner.run(pipe);
			shuffler.close();

			Source<CloneGroup> groups = Codecs.decode(shuffler, i.getInstance(Function2FineClonesCodec.class));
			for (Iterable<CloneGroup> row : groups) {
				for (CloneGroup cg : row) {
					System.out.println(cg.getText());
				}
			}
		}
	}
}
