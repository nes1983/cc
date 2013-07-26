package ch.unibe.scg.cc;

import static com.google.common.io.BaseEncoding.base16;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.unibe.scg.cells.Cell;
import org.unibe.scg.cells.CellSource;
import org.unibe.scg.cells.Codec;
import org.unibe.scg.cells.Codecs;
import org.unibe.scg.cells.InMemoryPipeline;
import org.unibe.scg.cells.InMemoryShuffler;

import ch.unibe.scg.cc.Annotations.Function2RoughClones;
import ch.unibe.scg.cc.Annotations.PopularSnippets;
import ch.unibe.scg.cc.Annotations.PopularSnippetsThreshold;
import ch.unibe.scg.cc.Annotations.Snippet2Functions;
import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.CloneType;
import ch.unibe.scg.cc.Protos.GitRepo;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.javaFrontend.JavaModule;

import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Modules;

@SuppressWarnings("javadoc")
public final class Function2RoughClonerTest {
	@Test
	public void testMap() throws IOException {
		Injector i = Guice.createInjector(
				Modules.override(new CCModule(), new JavaModule(), new InMemoryModule()).with(new TestModule()));
		Codec<GitRepo> repoCodec = i.getInstance(Key.get(new TypeLiteral<Codec<GitRepo>>() {}));
		CollectionCellSource<GitRepo> src = new CollectionCellSource<>(Arrays.<Iterable<Cell<GitRepo>>> asList(Arrays
				.asList(repoCodec.encode(GitPopulatorTest.parseZippedGit("paperExample.zip")))));

		try (InMemoryShuffler<Clone> sink = i.getInstance(Key.get(new TypeLiteral<InMemoryShuffler<Clone>>() {}))) {
			InMemoryPipeline.make(src, sink)
				.influx(repoCodec)
				.mapper(i.getProvider(GitPopulator.class))
				.shuffle(i.getInstance(Key.get(new TypeLiteral<Codec<Snippet>>() {}, Snippet2Functions.class)))
				.efflux(
						i.getProvider(Function2RoughCloner.class),
						i.getInstance(Key.get(new TypeLiteral<Codec<Clone>>() {}, Function2RoughClones.class)));

			// See paper: Table III
			assertThat(Iterables.size(sink), is(2));

			CellSource<Snippet> popularPartitions = i.getInstance(Key.get(new TypeLiteral<CellSource<Snippet>>() {}, PopularSnippets.class));

			List<Iterable<Snippet>> decodedRows = new ArrayList<>();
			for (Iterable<Cell<Snippet>> popularPartition : popularPartitions) {
			 	decodedRows.add(Codecs.decode(popularPartition, i.getInstance(
			 			Key.get((new TypeLiteral<Codec<Snippet>>() {}), PopularSnippets.class))));
			}

			Iterable<Snippet> d618 = null;
			for (Iterable<Snippet> row : decodedRows) {
				if (base16().encode(Iterables.get(row, 0).getFunction().toByteArray()).startsWith("D618")) {
					d618 = row;
					break;
				}
			}
			assert d618 != null; // Null analysis insists.
			assertNotNull(d618);

			Set<String> snippetHashes = new HashSet<>();
			for (Snippet s : d618) {
				if (s.getCloneType() == CloneType.GAPPED) {
					snippetHashes.add(base16().encode(s.getHash().toByteArray()));
				}
			}

			assertThat(snippetHashes.toString(),
					new HashSet<>(GitPopulatorTest.d618SnippetHashes()).containsAll(snippetHashes), is(true));
		}
		// TODO continue paper example
	}

	/** Bridge between Collections and CellSource */
	private static class CollectionCellSource<T> implements CellSource<T> {
		final private Iterable<Iterable<Cell<T>>> collection;

		CollectionCellSource(Iterable<Iterable<Cell<T>>> collection) {
			this.collection = collection;
		}

		@Override
		public Iterator<Iterable<Cell<T>>> iterator() {
			return collection.iterator();
		}
	}

	private static class TestModule extends AbstractModule {
		@Override
		protected void configure() {
			bindConstant().annotatedWith(PopularSnippetsThreshold.class).to(3);
		}
	}
}
