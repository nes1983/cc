package ch.unibe.scg.cc.modules;

import javax.inject.Singleton;

import org.apache.hadoop.hbase.client.HTable;

import ch.unibe.scg.cc.activerecord.HTableProvider;
import ch.unibe.scg.cc.activerecord.IPutFactory;
import ch.unibe.scg.cc.activerecord.PutFactory;
import ch.unibe.scg.cc.mappers.HTableWriteBuffer;
import ch.unibe.scg.cc.mappers.HTableWriteBuffer.BufferFactory;

import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
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
		bind(IPutFactory.class).to(PutFactory.class);
	}

	private void installHTable(final String tableName) {
		this.install(new HTableModule(tableName) {
			@Override
			void bindHTable() {
				bind(String.class).annotatedWith(Names.named("tableName")).toInstance(tableName);
			}
		});
	}

	static abstract class HTableModule extends PrivateModule {
		private final String named;

		HTableModule(String named) {
			this.named = named;
		}

		@Override
		protected void configure() {
			bind(HTable.class).annotatedWith(Names.named(named)).toProvider(HTableProvider.class).in(Singleton.class);
			expose(HTable.class).annotatedWith(Names.named(named));

			bindHTable();
		}

		abstract void bindHTable();
	}
}
