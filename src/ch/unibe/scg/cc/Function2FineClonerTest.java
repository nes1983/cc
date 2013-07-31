package ch.unibe.scg.cc;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;
import org.unibe.scg.cells.Cell;
import org.unibe.scg.cells.Codec;
import org.unibe.scg.cells.InMemoryPipeline;
import org.unibe.scg.cells.InMemoryShuffler;

import ch.unibe.scg.cc.Function2RoughClonerTest.CollectionCellSource;
import ch.unibe.scg.cc.Function2RoughClonerTest.TestModule;
import ch.unibe.scg.cc.Protos.CloneGroup;
import ch.unibe.scg.cc.Protos.GitRepo;
import ch.unibe.scg.cc.javaFrontend.JavaModule;

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
		Codec<GitRepo> repoCodec = i.getInstance(Key.get(new TypeLiteral<Codec<GitRepo>>() {}));
		CollectionCellSource<GitRepo> src = new CollectionCellSource<>(Arrays.<Iterable<Cell<GitRepo>>> asList(Arrays
				.asList(repoCodec.encode(GitPopulatorTest.parseZippedGit("paperExample.zip")))));

		ClonePipelineRunner runner = i.getInstance(ClonePipelineRunner.class);
		try (InMemoryShuffler<CloneGroup> sink =
				i.getInstance(Key.get(new TypeLiteral<InMemoryShuffler<CloneGroup>>() {}))) {
			InMemoryPipeline<GitRepo, CloneGroup> pipe = InMemoryPipeline.make(src, sink);
			runner.run(pipe);
		}
	}
}
