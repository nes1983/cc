package ch.unibe.scg.cc.modules;

import java.security.MessageDigest;
import java.util.Comparator;
import java.util.Set;

import javax.inject.Singleton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.MessageDigestProvider;
import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.ConfigurationProvider;
import ch.unibe.scg.cc.activerecord.FastPutFactory;
import ch.unibe.scg.cc.activerecord.HTableProvider;
import ch.unibe.scg.cc.activerecord.IPutFactory;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.RealCodeFile;
import ch.unibe.scg.cc.activerecord.RealCodeFileFactory;
import ch.unibe.scg.cc.activerecord.RealProject;
import ch.unibe.scg.cc.activerecord.RealProjectFactory;
import ch.unibe.scg.cc.activerecord.RealVersion;
import ch.unibe.scg.cc.activerecord.RealVersionFactory;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.javaFrontend.JavaType1ReplacerFactory;
import ch.unibe.scg.cc.mappers.GuiceMapper;
import ch.unibe.scg.cc.mappers.GuiceReducer;
import ch.unibe.scg.cc.mappers.IndexFiles2Versions.IndexFiles2VersionsMapper;
import ch.unibe.scg.cc.mappers.IndexFiles2Versions.IndexFiles2VersionsReducer;
import ch.unibe.scg.cc.mappers.IndexFunctions2Files.IndexFunctions2FilesMapper;
import ch.unibe.scg.cc.mappers.IndexFunctions2Files.IndexFunctions2FilesReducer;
import ch.unibe.scg.cc.mappers.IndexHashfacts2Functions.IndexHashfacts2FunctionsMapper;
import ch.unibe.scg.cc.mappers.IndexHashfacts2Functions.IndexHashfacts2FunctionsReducer;
import ch.unibe.scg.cc.mappers.IndexVersions2Projects.IndexVersions2ProjectsMapper;
import ch.unibe.scg.cc.mappers.IndexVersions2Projects.IndexVersions2ProjectsReducer;
import ch.unibe.scg.cc.regex.Replace;
import ch.unibe.scg.cc.util.ByteSetProvider;

import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

public class CCModule extends AbstractModule {
	
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
		
		bind(MessageDigest.class).toProvider(MessageDigestProvider.class).in(
				Singleton.class);
		bind(Replace[].class).annotatedWith(Names.named("Type1")).toProvider(
				JavaType1ReplacerFactory.class);
		bind(Configuration.class).toProvider(ConfigurationProvider.class).in(
				Singleton.class);
		bind(new TypeLiteral<Comparator<byte[]>>(){}).
				toInstance(Bytes.BYTES_COMPARATOR);
		bind(new TypeLiteral<Set<byte[]>>(){}).
				toProvider(ByteSetProvider.class);
		
		
		// MR
		bind(GuiceMapper.class).annotatedWith(Names.named("IndexVersions2ProjectsMapper")).to(IndexVersions2ProjectsMapper.class);
        bind(GuiceReducer.class).annotatedWith(Names.named("IndexVersions2ProjectsReducer")).to(IndexVersions2ProjectsReducer.class);
        bind(GuiceMapper.class).annotatedWith(Names.named("IndexFiles2VersionsMapper")).to(IndexFiles2VersionsMapper.class);
        bind(GuiceReducer.class).annotatedWith(Names.named("IndexFiles2VersionsReducer")).to(IndexFiles2VersionsReducer.class);
        bind(GuiceMapper.class).annotatedWith(Names.named("IndexFunctions2FilesMapper")).to(IndexFunctions2FilesMapper.class);
        bind(GuiceReducer.class).annotatedWith(Names.named("IndexFunctions2FilesReducer")).to(IndexFunctions2FilesReducer.class);
        bind(GuiceMapper.class).annotatedWith(Names.named("IndexHashfacts2FunctionsMapper")).to(IndexHashfacts2FunctionsMapper.class);
        bind(GuiceReducer.class).annotatedWith(Names.named("IndexHashfacts2FunctionsReducer")).to(IndexHashfacts2FunctionsReducer.class);
        
        
        // factories
		install(new FactoryModuleBuilder().implement(Project.class,
				RealProject.class).build(RealProjectFactory.class));
		install(new FactoryModuleBuilder().implement(Version.class,
				RealVersion.class).build(RealVersionFactory.class));
		install(new FactoryModuleBuilder().implement(CodeFile.class,
				RealCodeFile.class).build(RealCodeFileFactory.class));
		bind(IPutFactory.class).to(FastPutFactory.class);
		
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
