package ch.unibe.scg.cc;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.Test;

import ch.unibe.scg.cc.Annotations.Function2RoughClones;
import ch.unibe.scg.cc.Annotations.Snippet2Functions;
import ch.unibe.scg.cc.Protos.Clone;
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
		try(ZippedGit testRepo = GitPopulatorTest.parseZippedGit("paperExample.zip")) {
			GitPopulatorTest.walkRepo(i, testRepo);
		}

		MapperRunner mr = i.getInstance(MapperRunner.class);
		try (CellSink<Clone> f2rcSink =
				i.getInstance(Key.get(new TypeLiteral<CellSink<Clone>>() {}, Function2RoughClones.class))) {
			mr.run(i.getProvider(Function2RoughCloner.class),
					i.getInstance(Key.get(new TypeLiteral<CellSource<Snippet>>() {}, Snippet2Functions.class)),
					f2rcSink,
					i.getInstance(Key.get(new TypeLiteral<Codec<Snippet>>() {}, Snippet2Functions.class)),
					i.getInstance(Key.get(new TypeLiteral<Codec<Clone>>() {}, Function2RoughClones.class)));
		}

		CellSource<Clone> f2rcSource = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Clone>>() {}, Function2RoughClones.class));
		// See paper: Table III
		assertThat(Iterables.size(f2rcSource.partitions()), is(2));

		Iterable<Cell<Clone>> clonesFun1 = Iterables.get(f2rcSource.partitions(), 0);
		assertThat(Iterables.size(clonesFun1), is(5));

		// TODO continue paper example
	}
}
