package ch.unibe.scg.cc.modules;

import java.security.MessageDigest;

import javax.inject.Singleton;

import org.apache.hadoop.hbase.client.HTable;

import ch.unibe.scg.cc.MessageDigestProvider;
import ch.unibe.scg.cc.activerecord.HTableProvider;
import ch.unibe.scg.cc.javaFrontend.JavaType1ReplacerFactory;
import ch.unibe.scg.cc.regex.Replace;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class CCModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(MessageDigest.class).toProvider(MessageDigestProvider.class).in(Singleton.class);
		bind(Replace[].class).annotatedWith(Names.named("Type1"))
		.toProvider(JavaType1ReplacerFactory.class);
		bind(String.class).annotatedWith(Names.named("tableName"))
			.toInstance("facts");
		bind(HTable.class).toProvider(HTableProvider.class).in(Singleton.class);
	}

}
