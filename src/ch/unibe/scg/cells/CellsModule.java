package ch.unibe.scg.cells;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.inject.util.Types.newParameterizedType;
import static com.google.inject.util.Types.newParameterizedTypeWithOwner;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;

import javax.inject.Inject;
import javax.inject.Provider;

import ch.unibe.scg.cells.Annotations.FamilyName;
import ch.unibe.scg.cells.Annotations.TableName;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Types;
import com.google.protobuf.ByteString;

/**
 * Default bindings for use with cells. binds Sources to encoded CellSources, and sinks
 * to encoded CellSinks.
 *
 * <p>
 * For this module to work, there need to be bound providers for these exact types: <br>
 * {@code CellSource<?>, CellSink<?>, CellLookupTable<?>}.
 */
public abstract class CellsModule extends AbstractModule {
	private static class SourceProvider<T> implements Provider<Source<T>> {
		final private Provider<CellSource<?>> cellSrc;
		final private Provider<Codec<T>> codec;

		@Inject
		SourceProvider(Provider<CellSource<?>> cellSrc, Provider<Codec<T>> codec) {
			this.codec = codec;
			this.cellSrc = cellSrc;
		}

		@Override
		public Source<T> get() {
			return Codecs.decode(((CellSource<T>) cellSrc.get()), codec.get());
		}
	}

	private static class SinkProvider<T> implements Provider<Sink<T>> {
		final private Provider<CellSink<?>> sink;
		final private Provider<Codec<T>> codec;

		@Inject
		SinkProvider(Provider<CellSink<?>> sink, Provider<Codec<T>> codec) {
			this.codec = codec;
			this.sink = sink;
		}

		@Override
		public Sink<T> get() {
			return Codecs.encode(((CellSink<T>) sink.get()), codec.get());
		}
	}

	private static class LookupTableProvider<T> implements Provider<LookupTable<T>> {
		final private Provider<CellLookupTable<?>> rawCellTable;
		final private Provider<Codec<T>> codec;

		@Inject
		LookupTableProvider(Provider<CellLookupTable<?>> rawCellTable, Provider<Codec<T>> codec) {
			this.rawCellTable = rawCellTable;
			this.codec = codec;
		}

		@Override
		public LookupTable<T> get() {
			return Codecs.decodedTable((CellLookupTable<T>) rawCellTable.get(), codec.get());
		}
	}

	// TODO: Use builder pattern for 6 parameters.
	protected final <T> void installTable(final String tableName, final ByteString family,
			final Class<? extends Annotation> annotation, final TypeLiteral<T> lit,
			final Class<? extends Codec<T>> codec, final StorageModule storageModule) {
		checkNotNull(tableName);
		checkNotNull(family);
		checkNotNull(annotation);
		checkNotNull(lit);
		checkNotNull(codec);
		checkNotNull(storageModule);

		install(new PrivateModule() {
			@Override protected void configure() {
				install(storageModule);

				bind((Key<Codec<T>>) Key.get(Types.newParameterizedType(Codec.class, lit.getType()))).to(codec);
				bindConstant().annotatedWith(TableName.class).to(tableName);

				bind(ByteString.class).annotatedWith(FamilyName.class).toInstance(family);

				bind((Key<Source<T>>) Key.get(newParameterizedType(Source.class, lit.getType()))).toProvider(
						(Key<Provider<Source<T>>>) Key.get(newParameterizedTypeWithOwner(CellsModule.class,
								SourceProvider.class, lit.getType())));
				bind((Key<Sink<T>>) Key.get(newParameterizedType(Sink.class, lit.getType()))).toProvider(
						(Key<Provider<Sink<T>>>) Key.get(newParameterizedTypeWithOwner(CellsModule.class, SinkProvider.class,
								lit.getType())));
				bind((Key<LookupTable<T>>) Key.get(newParameterizedType(LookupTable.class, lit.getType()))).toProvider(
						(Key<Provider<LookupTable<T>>>) Key.get(newParameterizedTypeWithOwner(CellsModule.class,
								LookupTableProvider.class, lit.getType())));

				ParameterizedType src = Types.newParameterizedType(Source.class, lit.getType());
				Key<Source<T>> exposedSrc = (Key<Source<T>>) Key.get(src, annotation);
				bind(exposedSrc).to((Key<? extends Source<T>>) Key.get(src));
				expose(exposedSrc);

				ParameterizedType sink = Types.newParameterizedType(Sink.class, lit.getType());
				Key<Sink<T>> exposedSink = (Key<Sink<T>>) Key.get(sink, annotation);
				bind(exposedSink).to((Key<? extends Sink<T>>) Key.get(sink));
				expose(exposedSink);

				ParameterizedType lookup = Types.newParameterizedType(LookupTable.class, lit.getType());
				Key<LookupTable<T>> exposedLookup = (Key<LookupTable<T>>) Key.get(lookup, annotation);
				bind(exposedLookup).to((Key<? extends LookupTable<T>>) Key.get(lookup));
				expose(exposedLookup);
			}
		});
	}
}
