package ch.unibe.scg.cc.modules;

import java.security.MessageDigest;

import javax.inject.Singleton;

import ch.unibe.scg.cc.MessageDigestProvider;
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
	}

}
