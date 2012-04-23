package ch.unibe.scg.cc.modules;

import java.security.MessageDigest;

import javax.inject.Singleton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;

import ch.unibe.scg.cc.MessageDigestProvider;
import ch.unibe.scg.cc.activerecord.HTableProvider;
import ch.unibe.scg.cc.activerecord.RealCodeFile;
import ch.unibe.scg.cc.activerecord.RealCodeFileFactory;
import ch.unibe.scg.cc.activerecord.ConfigurationProvider;
import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.activerecord.RealProject;
import ch.unibe.scg.cc.activerecord.RealProjectFactory;
import ch.unibe.scg.cc.activerecord.RealVersion;
import ch.unibe.scg.cc.activerecord.RealVersionFactory;
import ch.unibe.scg.cc.javaFrontend.JavaType1ReplacerFactory;
import ch.unibe.scg.cc.regex.Replace;

import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
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
		
		bind(MessageDigest.class).toProvider(MessageDigestProvider.class).in(
				Singleton.class);
		bind(Replace[].class).annotatedWith(Names.named("Type1")).toProvider(
				JavaType1ReplacerFactory.class);
		bind(Configuration.class).toProvider(ConfigurationProvider.class).in(
				Singleton.class);

		install(new FactoryModuleBuilder().implement(Project.class,
				RealProject.class).build(RealProjectFactory.class));
		install(new FactoryModuleBuilder().implement(Version.class,
				RealVersion.class).build(RealVersionFactory.class));
		install(new FactoryModuleBuilder().implement(CodeFile.class,
				RealCodeFile.class).build(RealCodeFileFactory.class));
		
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
