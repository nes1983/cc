package ch.unibe.scg.cells;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.inject.util.Types.newParameterizedType;
import static com.google.inject.util.Types.newParameterizedTypeWithOwner;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;

import javax.inject.Inject;
import javax.inject.Provider;

import ch.unibe.scg.cells.CounterModule.CounterName;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Types;

/**
 * An API that helps configure Cells jobs. Helps configure tables, sources, sinks, and counters.
 * To configure your cells job, just subclass CellsModule, and use helper methods.
 */
public abstract class CellsModule extends AbstractModule {
	private static class SourceProvider<T> implements Provider<Source<T>> {

		@Inject
		SourceProvider() { }

		@Override
		public Source<T> get() {
			throw new RuntimeException("We're intending to delete sources.");
		}
	}

	private static class SinkProvider<T> implements Provider<Sink<T>> {
		final private Provider<CellSink<Void>> sink;
		final private Provider<Codec<T>> codec;

		@Inject
		SinkProvider(Provider<CellSink<Void>> sink, Provider<Codec<T>> codec) {
			this.codec = codec;
			this.sink = sink;
		}

		@Override
		public Sink<T> get() {
			return Codecs.encode(((CellSink<T>) sink.get()), codec.get());
		}
	}

	private static class LookupTableProvider<T> implements Provider<LookupTable<T>> {
		final private Provider<CellLookupTable<Void>> rawCellTable;
		final private Provider<Codec<T>> codec;

		@Inject
		LookupTableProvider(Provider<CellLookupTable<Void>> rawCellTable, Provider<Codec<T>> codec) {
			this.rawCellTable = rawCellTable;
			this.codec = codec;
		}

		@Override
		public LookupTable<T> get() {
			return Codecs.decodedTable((CellLookupTable<T>) rawCellTable.get(), codec.get());
		}
	}

	/**
	 * Example:
	 * <pre>
	 * {@code
		Module tab = new CellsModule() {
			@Override
			protected void configure() {
				installTable(
						In.class,
						new TypeLiteral<Act>() {},
						ActCodec.class,
						new HBaseStorage(), new HBaseTableModule(TABLE_NAME_IN,	fam));
				installTable(
						Eff.class,
						new TypeLiteral<WordCount>() {},
						WordCountCodec.class,
						new HBaseStorage(), new HBaseTableModule(TABLE_NAME_EFF, fam));
			}
		};
	 *}
	 *</pre>
	 *
	 * <p>
	 * Once you've got that, you can simply ask for the proper Sink, Source, or LookupTable
	 * to be injected. Example:
	 * <pre> {@code
	 * @Inject
	 * MyConstructor(@In LookupTable<Act> actLookup, @Eff Sink<WordCount>) {
	 *    …
	 * }
	 * } </pre>
	 *
	 * @param tableModule The information needed to initialize this specific table.
	 * 	That's things like table name and column family, for the case of HBase tables.
	 *  In memory tables don't really need this, so for in memory tables, just pass an empty TableModule.
	 *  If you don't know what you want, pass in an HBaseTableModule. Then, you can create both
	 *  in memory tables, and HBase tables.
	 *
	 */
	@SuppressWarnings("javadoc") // Yea, fuck that. For javadoc to be happy, I have to escape "@", "{", "}", "<", ">".
                                 // That makes copy-pasting from code impossible.
	protected final <T> void installTable(
			final Class<? extends Annotation> annotation, final TypeLiteral<T> lit,
			final Class<? extends Codec<T>> codec, final StorageModule storageModule,
					final TableModule tableModule) {
		checkNotNull(annotation);
		checkNotNull(lit);
		checkNotNull(codec);
		checkNotNull(storageModule);

		install(new PrivateModule() {
			@Override protected void configure() {
				install(storageModule);
				install(tableModule);

				bind((Key<Codec<T>>) Key.get(Types.newParameterizedType(Codec.class, lit.getType()))).to(codec);

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

	/**
	 * Installs a counter into the module, so that the following will work for an
	 * annotation {@code @IOExceptions}:
	 *
	 * <pre> {@code
	 * @Inject
	 * MyConstructor(@IOExceptions Counter ioExceptionsCounter) {
	 *   …
	 * }
	 * }</pre>
	 */
	@SuppressWarnings("javadoc") // See above.
	protected final void installCounter(final Class<? extends Annotation> annotation, final CounterModule pipelineModule) {
		checkNotNull(annotation);
		checkNotNull(pipelineModule);

		install(new PrivateModule() {
			@Override protected void configure() {
				install(pipelineModule);

				bindConstant().annotatedWith(CounterName.class).to(annotation.getName());
				bind(Counter.class).annotatedWith(annotation).to(Counter.class);
				expose(Counter.class).annotatedWith(annotation);
			}
		});
	}
}
