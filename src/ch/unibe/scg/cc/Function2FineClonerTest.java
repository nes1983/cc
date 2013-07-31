package ch.unibe.scg.cc;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;
import org.unibe.scg.cells.Cell;
import org.unibe.scg.cells.Codec;
import org.unibe.scg.cells.InMemoryPipeline;
import org.unibe.scg.cells.InMemoryShuffler;

import ch.unibe.scg.cc.Annotations.Function2FineClones;
import ch.unibe.scg.cc.Annotations.Function2RoughClones;
import ch.unibe.scg.cc.Annotations.Snippet2Functions;
import ch.unibe.scg.cc.Function2RoughClonerTest.CollectionCellSource;
import ch.unibe.scg.cc.Function2RoughClonerTest.TestModule;
import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.CloneGroup;
import ch.unibe.scg.cc.Protos.GitRepo;
import ch.unibe.scg.cc.Protos.Snippet;
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
		Injector i = Guice.createInjector(
				Modules.override(new CCModule(), new JavaModule(), new InMemoryModule()).with(new TestModule()));
		Codec<GitRepo> repoCodec = i.getInstance(Key.get(new TypeLiteral<Codec<GitRepo>>() {}));
		CollectionCellSource<GitRepo> src = new CollectionCellSource<>(Arrays.<Iterable<Cell<GitRepo>>> asList(Arrays
				.asList(repoCodec.encode(GitPopulatorTest.parseZippedGit("paperExample.zip")))));

		try (InMemoryShuffler<CloneGroup> sink = i.getInstance(Key.get(new TypeLiteral<InMemoryShuffler<CloneGroup>>() {}))) {
			InMemoryPipeline.make(src, sink)
				.influx(repoCodec)
				.mapper(i.getProvider(GitPopulator.class))
				.shuffle(i.getInstance(Key.get(new TypeLiteral<Codec<Snippet>>() {}, Snippet2Functions.class)))
				.mapper(i.getProvider(Function2RoughCloner.class))
				.shuffle(i.getInstance(Key.get(new TypeLiteral<Codec<Clone>>() {}, Function2RoughClones.class)))
				.efflux(i.getProvider(Function2FineCloner.class),
						i.getInstance(Key.get(new TypeLiteral<Codec<CloneGroup>>() {}, Function2FineClones.class)));
		}
	}
}
