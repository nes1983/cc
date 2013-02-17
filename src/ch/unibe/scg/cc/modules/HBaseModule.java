package ch.unibe.scg.cc.modules;

import javax.inject.Singleton;

import org.apache.hadoop.hbase.client.HTable;

import ch.unibe.scg.cc.activerecord.HTableProvider;
import ch.unibe.scg.cc.activerecord.IPutFactory;
import ch.unibe.scg.cc.activerecord.PutFactory;
import ch.unibe.scg.cc.mappers.GitTablePopulator.GitTablePopulatorMapper;
import ch.unibe.scg.cc.mappers.GuiceMapper;
import ch.unibe.scg.cc.mappers.GuiceReducer;
import ch.unibe.scg.cc.mappers.GuiceTableMapper;
import ch.unibe.scg.cc.mappers.GuiceTableReducer;
import ch.unibe.scg.cc.mappers.HTableWriteBuffer;
import ch.unibe.scg.cc.mappers.HTableWriteBuffer.BufferFactory;
import ch.unibe.scg.cc.mappers.Histogram.HistogramMapper;
import ch.unibe.scg.cc.mappers.Histogram.HistogramReducer;
import ch.unibe.scg.cc.mappers.IndexFacts2Functions.IndexFacts2FunctionsMapper;
import ch.unibe.scg.cc.mappers.IndexFacts2Functions.IndexFacts2FunctionsReducer;
import ch.unibe.scg.cc.mappers.IndexFacts2FunctionsStep2.IndexFacts2FunctionsStep2Mapper;
import ch.unibe.scg.cc.mappers.IndexFacts2FunctionsStep2.IndexFacts2FunctionsStep2Reducer;
import ch.unibe.scg.cc.mappers.TablePopulator.TablePopulatorMapper;

import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

public class HBaseModule extends AbstractModule {

	@Override
	protected void configure() {
		installHTable("versions");
		installHTable("files");
		installHTable("functions");
		installHTable("facts");
		installHTable("strings");

		installHTable("hashfactContent");

		installHTable("indexFacts2Functions");
		installHTable("indexFacts2FunctionsStep2");

		installHTable("indexVersions2Projects");
		installHTable("indexFiles2Versions");
		installHTable("indexFunctions2Files");
		installHTable("indexHashfacts2Functions");

		installHTable("indexFactToProject");

		// MR
		bind(GuiceMapper.class).annotatedWith(Names.named(TablePopulatorMapper.class.getName())).to(
				TablePopulatorMapper.class);
		bind(GuiceMapper.class).annotatedWith(Names.named(GitTablePopulatorMapper.class.getName())).to(
				GitTablePopulatorMapper.class);
		bind(GuiceTableMapper.class).annotatedWith(Names.named(IndexFacts2FunctionsMapper.class.getName())).to(
				IndexFacts2FunctionsMapper.class);
		bind(GuiceTableReducer.class).annotatedWith(Names.named(IndexFacts2FunctionsReducer.class.getName())).to(
				IndexFacts2FunctionsReducer.class);
		bind(GuiceTableMapper.class).annotatedWith(Names.named(IndexFacts2FunctionsStep2Mapper.class.getName())).to(
				IndexFacts2FunctionsStep2Mapper.class);
		bind(GuiceTableReducer.class).annotatedWith(Names.named(IndexFacts2FunctionsStep2Reducer.class.getName())).to(
				IndexFacts2FunctionsStep2Reducer.class);
		bind(GuiceTableMapper.class).annotatedWith(Names.named(HistogramMapper.class.getName())).to(
				HistogramMapper.class);
		bind(GuiceReducer.class).annotatedWith(Names.named(HistogramReducer.class.getName()))
				.to(HistogramReducer.class);

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
