package ch.unibe.scg.cc;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

import ch.unibe.scg.cc.Annotations.Function2RoughClones;
import ch.unibe.scg.cc.Annotations.Snippet2Functions;
import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.GitRepo;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.javaFrontend.JavaModule;

import com.google.common.collect.Iterables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

@SuppressWarnings("javadoc")
public final class Function2RoughClonerTest {
	@Test
	public void testMap() throws IOException {
		Injector i = Guice.createInjector(new CCModule(), new JavaModule(), new InMemoryModule());
		Codec<GitRepo> repoCodec = i.getInstance(Key.get(new TypeLiteral<Codec<GitRepo>>() {}));
		CollectionCellSource<GitRepo> src = new CollectionCellSource<>(Arrays.<Iterable<Cell<GitRepo>>> asList(Arrays
				.asList(repoCodec.encode(GitPopulatorTest.parseZippedGit("paperExample.zip")))));

		try (InMemoryShuffler<Clone> sink = i.getInstance(Key.get(new TypeLiteral<InMemoryShuffler<Clone>>() {}))) {
			new InMemoryPipeline<>(src, sink)
				.influx(repoCodec)
				.mapper(i.getProvider(GitPopulator.class))
				.shuffle(i.getInstance(Key.get(new TypeLiteral<Codec<Snippet>>() {}, Snippet2Functions.class)))
				.efflux(
						i.getProvider(Function2RoughCloner.class),
						i.getInstance(Key.get(new TypeLiteral<Codec<Clone>>() {}, Function2RoughClones.class)));

			// See paper: Table III
			assertThat(Iterables.size(sink), is(2));

			Iterable<Cell<Clone>> clonesFun1 = Iterables.get(sink, 0);
			assertThat(Iterables.size(clonesFun1), is(5));
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
}
