package ch.unibe.scg.cc;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

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
import ch.unibe.scg.cells.InMemoryStorage;
import ch.unibe.scg.cells.Source;

import com.google.common.collect.Iterables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Modules;

/** Test {@link CloneGroupClusterer} */
public final class CloneGroupClustererTest {
	/** Test {@link CloneGroupClusterer#map} */
	@Test
	public void testMap() throws IOException, InterruptedException {
		Injector i = Guice.createInjector(
				Modules.override(new CCModule(new InMemoryStorage()), new JavaModule()).with(new TestModule()));
		Codec<GitRepo> repoCodec = i.getInstance(GitRepoCodec.class);
		CollectionCellSource<GitRepo> src = new CollectionCellSource<>(Arrays.<Iterable<Cell<GitRepo>>> asList(Arrays
				.asList(repoCodec.encode(GitPopulatorTest.parseZippedGit("paperExample.zip")))));

		try (InMemoryShuffler<CloneGroup> sink = i.getInstance(Key.get(new TypeLiteral<InMemoryShuffler<CloneGroup>>() {}))) {
			InMemoryPipeline.make(src, sink)
				.influx(repoCodec)
				.mapper(i.getInstance(GitPopulator.class))
				.shuffle(i.getInstance(Snippet2FunctionsCodec.class))
				.mapper(i.getInstance(Function2RoughCloner.class))
				.shuffle(i.getInstance(Function2RoughClonesCodec.class))
				.mapper(i.getInstance(Function2FineCloner.class))
				.shuffle(i.getInstance(Function2RoughClonesCodec.class))
				.effluxWithOfflineMapper(i.getInstance(CloneGroupClusterer.class),
						i.getInstance(CloneGroupCodec.class));

			sink.close();

			try(Source<CloneGroup> source = Codecs.decode(sink, i.getInstance(CloneGroupCodec.class))) {
				// three identical matches in two tags
				assertThat(Iterables.get(Iterables.get(source, 0), 0).getOccurrencesList().size(), is(2*3));
			}
		}
	}
}
