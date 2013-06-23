package ch.unibe.scg.cc.mappers;

import javax.inject.Singleton;

import org.apache.hadoop.hbase.client.HTable;

import ch.unibe.scg.cc.activerecord.HTableProvider;
import ch.unibe.scg.cc.mappers.HTableWriteBuffer.BufferFactory;

import com.google.common.cache.LoadingCache;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

public class HBaseModule extends AbstractModule {
	@Override
	protected void configure() {
		installHTable("project2version");
		installHTable("version2file");
		installHTable("file2function");
		installHTable("function2snippet");
		installHTable("strings");

		installHTable("snippet2function");
		installHTable("function2roughclones");
		installHTable("popularSnippets");
		installHTable("function2fineclones");

		installHTable("duplicateSnippetsPerFunction");

		install(new FactoryModuleBuilder().implement(HTableWriteBuffer.class, HTableWriteBuffer.class).build(
				BufferFactory.class));

		bind(new TypeLiteral<LoadingCache<byte[], String>>() {})
			.annotatedWith(CloneLoaderProvider.CloneLoader.class)
			.toProvider(CloneLoaderProvider.class)
			.in(Singleton.class);
	}

	private void installHTable(final String tableName) {
		this.install(new HTableModule(tableName) {
			@Override
			void bindHTable() {
				bind(String.class).annotatedWith(Names.named("tableName")).toInstance(tableName);
			}
		});
	}

	/**
	 * This is a bit of a contraption to make table injection easy.
	 * It would have been much easier to leave it at a HTableFactory
	 * and a HTableWriteBufferFactory.
	 *
	 * <p>The advantage of the contraption is that -- from the client perspective,
	 * it's actually quite nice. You can just do
	 *
	 * {@code @Inject @Named("popularSnippets") HTable popularSnippets; }
	 *
	 * and that'll work.
	 *
	 * <p>Without the contraption, clients needing a popularSnippetsTable would have to choose
	 * between calling factory methods in their constructors, or all getting very
	 * similar providers that call just one method. (That's probably what we should have done.)
	 *
	 * @author nes
	 */
	static abstract class HTableModule extends PrivateModule {
		private final String named;

		HTableModule(String named) {
			this.named = named;
		}

		@Override
		protected void configure() {
			bind(HTable.class).annotatedWith(Names.named(named)).toProvider(HTableProvider.class).in(Singleton.class);
			bind(HTableWriteBuffer.class).annotatedWith(Names.named(named)).toProvider(
					HTableWriteBuffer.HTableWriteBufferProvider.class);

			expose(HTable.class).annotatedWith(Names.named(named));
			expose(HTableWriteBuffer.class).annotatedWith(Names.named(named));

			bindHTable();
		}

		abstract void bindHTable();
	}
}
