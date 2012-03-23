package ch.unibe.scg.cc.modules;

import java.security.MessageDigest;

import javax.inject.Singleton;

import org.apache.hadoop.conf.Configuration;

import ch.unibe.scg.cc.MessageDigestProvider;
import ch.unibe.scg.cc.activerecord.ConfigurationProvider;
import ch.unibe.scg.cc.javaFrontend.JavaType1ReplacerFactory;
import ch.unibe.scg.cc.regex.Replace;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class CCModule extends AbstractModule {

	public CCModule() {
		
	}
	
	@Override
	protected void configure() {
		installHTable("projects");
		installHTable("versions");
		installHTable("files");
		installHTable("functions");
		installHTable("strings");
		
		bind(MessageDigest.class).toProvider(MessageDigestProvider.class).in(Singleton.class);
		bind(Replace[].class).annotatedWith(Names.named("Type1"))
			.toProvider(JavaType1ReplacerFactory.class);
//		bind(HTable.class).annotatedWith(Names.named("facts")).toProvider(HTableProvider.class).in(Singleton.class);
//		bind(HTable.class).annotatedWith(Names.named("strings")).toProvider(HTableProvider.class).in(Singleton.class);
		bind(Configuration.class).toProvider(ConfigurationProvider.class).in(Singleton.class);
		
	}

	private void installHTable(final String tableName) {
		this.install(new HTableModule(tableName) {
			@Override
			void bindHTable() {
				bind(String.class).annotatedWith(Names.named("tableName")).toInstance(tableName);		
			}
		});
	}
}
