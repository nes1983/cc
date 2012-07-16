package ch.unibe.scg.cc.modules;

import javax.inject.Singleton;

import org.apache.hadoop.hbase.client.HTable;

import ch.unibe.scg.cc.activerecord.HTableProvider;
import ch.unibe.scg.cc.activerecord.IPutFactory;
import ch.unibe.scg.cc.activerecord.PutFactory;
import ch.unibe.scg.cc.mappers.GitTablePopulator.GitTablePopulatorMapper;
import ch.unibe.scg.cc.mappers.GuiceMapper;
import ch.unibe.scg.cc.mappers.GuiceTableMapper;
import ch.unibe.scg.cc.mappers.GuiceTableReducer;
import ch.unibe.scg.cc.mappers.HTableWriteBuffer;
import ch.unibe.scg.cc.mappers.HTableWriteBuffer.BufferFactory;
import ch.unibe.scg.cc.mappers.IndexFiles2Versions.IndexFiles2VersionsMapper;
import ch.unibe.scg.cc.mappers.IndexFiles2Versions.IndexFiles2VersionsReducer;
import ch.unibe.scg.cc.mappers.IndexFunctions2Files.IndexFunctions2FilesMapper;
import ch.unibe.scg.cc.mappers.IndexFunctions2Files.IndexFunctions2FilesReducer;
import ch.unibe.scg.cc.mappers.IndexHashfacts2Functions.IndexHashfacts2FunctionsMapper;
import ch.unibe.scg.cc.mappers.IndexHashfacts2Functions.IndexHashfacts2FunctionsReducer;
import ch.unibe.scg.cc.mappers.IndexVersions2Projects.IndexVersions2ProjectsMapper;
import ch.unibe.scg.cc.mappers.IndexVersions2Projects.IndexVersions2ProjectsReducer;
import ch.unibe.scg.cc.mappers.TablePopulator.TablePopulatorMapper;

import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

public class HBaseModule extends AbstractModule {

	@Override
	protected void configure() {
		installHTable("projects");
		installHTable("versions");
		installHTable("files");
		installHTable("functions");
		installHTable("strings");
		
		installHTable("hashfactContent");
		
		installHTable("indexVersions2Projects");
		installHTable("indexFiles2Versions");
		installHTable("indexFunctions2Files");
		installHTable("indexHashfacts2Functions");
		
		installHTable("indexFactToProject");
		

		// MR
		bind(GuiceMapper.class).annotatedWith(Names.named("TablePopulatorMapper")).to(TablePopulatorMapper.class);
		bind(GuiceMapper.class).annotatedWith(Names.named("GitTablePopulatorMapper")).to(GitTablePopulatorMapper.class);
        bind(GuiceTableMapper.class).annotatedWith(Names.named("IndexVersions2ProjectsMapper")).to(IndexVersions2ProjectsMapper.class);
        bind(GuiceTableReducer.class).annotatedWith(Names.named("IndexVersions2ProjectsReducer")).to(IndexVersions2ProjectsReducer.class);
        bind(GuiceTableMapper.class).annotatedWith(Names.named("IndexFiles2VersionsMapper")).to(IndexFiles2VersionsMapper.class);
        bind(GuiceTableReducer.class).annotatedWith(Names.named("IndexFiles2VersionsReducer")).to(IndexFiles2VersionsReducer.class);
        bind(GuiceTableMapper.class).annotatedWith(Names.named("IndexFunctions2FilesMapper")).to(IndexFunctions2FilesMapper.class);
        bind(GuiceTableReducer.class).annotatedWith(Names.named("IndexFunctions2FilesReducer")).to(IndexFunctions2FilesReducer.class);
        bind(GuiceTableMapper.class).annotatedWith(Names.named("IndexHashfacts2FunctionsMapper")).to(IndexHashfacts2FunctionsMapper.class);
        bind(GuiceTableReducer.class).annotatedWith(Names.named("IndexHashfacts2FunctionsReducer")).to(IndexHashfacts2FunctionsReducer.class);
        

		install(new FactoryModuleBuilder().implement(HTableWriteBuffer.class,
				HTableWriteBuffer.class).build(BufferFactory.class));
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
			bind(HTable.class).annotatedWith(Names.named(named))
					.toProvider(HTableProvider.class).in(Singleton.class);
			expose(HTable.class).annotatedWith(Names.named(named));

			bindHTable();
		}

		abstract void bindHTable();
	}
}
