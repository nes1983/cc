package ch.unibe.scg.cc;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Arrays;

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

import com.google.common.collect.Iterables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

/** Test {@link CloneGroupClusterer} */
public final class CloneGroupClustererTest {
	/** Test {@link CloneGroupClusterer#map} */
	@Test
	public void testMap() throws IOException, InterruptedException {
		Injector i = Guice.createInjector(
				Modules.override(new CCModule(new InMemoryStorage(), new LocalCounterModule()))
						.with(new TestModule()), new LocalExecutionModule());
		Codec<GitRepo> repoCodec = i.getInstance(GitRepoCodec.class);

		try (CellSource<GitRepo> src
					= Cells.shard(Cells.encode(
							Arrays.asList(GitPopulatorTest.parseZippedGit("paperExample.zip")),
							repoCodec));
				InMemoryPipeline<GitRepo, Clone> pipe = i.getInstance(InMemoryPipeline.Builder.class).make(src)) {
			pipe
				.influx(repoCodec)
				.map(i.getInstance(GitPopulator.class))
				.shuffle(i.getInstance(Snippet2FunctionsCodec.class))
				.map(i.getInstance(Function2RoughCloner.class))
				.shuffle(i.getInstance(Function2RoughClonesCodec.class))
				.mapAndEfflux(i.getInstance(Function2FineCloner.class),
						i.getInstance(Function2RoughClonesCodec.class));

			// TODO: run: .effluxWithOfflineMapper(i.getInstance(CloneGroupClusterer.class),

			try(Source<Clone> source
					= Cells.decodeSource(pipe.lastEfflux(), i.getInstance(Function2RoughClonesCodec.class))) {
				assertThat(Iterables.toString(source), is(""));
			}

			// three identical matches in two tags
			// TODO: check clustering: assertThat(Iterables.get(Iterables.get(source, 0), 0).getOccurrencesList().size(), is(2*3));
		}
	}
}
