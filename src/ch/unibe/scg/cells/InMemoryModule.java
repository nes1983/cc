package ch.unibe.scg.cells;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

/** A {@link StorageModule} that runs completely in memory. No files are touched. */
public final class InMemoryModule extends AbstractModule implements StorageModule {
	@Override
	protected void configure() {
		bind(new TypeLiteral<InMemoryShuffler<?>>() {}).in(Singleton.class);

		bind(new TypeLiteral<CellSource<?>>() {}).to(new TypeLiteral<InMemoryShuffler<?>>() {});
		bind(new TypeLiteral<CellSink<?>>() {}).to(new TypeLiteral<InMemoryShuffler<?>>() {});
		bind(new TypeLiteral<CellLookupTable<?>>() {}).to(new TypeLiteral<InMemoryShuffler<?>>() {});
	}
}
