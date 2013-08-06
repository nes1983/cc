package ch.unibe.scg.cells;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

/** A {@link StorageModule} that runs completely in memory. No files are touched. */
public final class InMemoryStorage extends AbstractModule implements StorageModule {
	@Override
	protected void configure() {
		bind(new TypeLiteral<InMemoryShuffler<Void>>() {}).in(Singleton.class);

		bind(new TypeLiteral<CellSource<Void>>() {}).to(new TypeLiteral<InMemoryShuffler<Void>>() {});
		bind(new TypeLiteral<CellSink<Void>>() {}).to(new TypeLiteral<InMemoryShuffler<Void>>() {});
		bind(new TypeLiteral<CellLookupTable<Void>>() {}).to(new TypeLiteral<InMemoryShuffler<Void>>() {});
	}
}
