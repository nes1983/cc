package ch.unibe.scg.cc.modules;

import java.security.MessageDigest;

import javax.inject.Singleton;

import ch.unibe.scg.cc.MessageDigestProvider;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.FunctionProvider;
import ch.unibe.scg.cc.activerecord.HashFact;
import ch.unibe.scg.cc.activerecord.HashFactProvider;
import ch.unibe.scg.cc.activerecord.Location;
import ch.unibe.scg.cc.activerecord.LocationProvider;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.ProjectProvider;
import ch.unibe.scg.cc.javaFrontend.JavaType1ReplacerFactory;
import ch.unibe.scg.cc.regex.Replace;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

public class CCModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(MessageDigest.class).toProvider(MessageDigestProvider.class).in(Singleton.class);
		bind(Replace[].class).annotatedWith(Names.named("Type1"))
		.toProvider(JavaType1ReplacerFactory.class);
		bind(Function.class).toProvider(FunctionProvider.class);
		bind(HashFact.class).toProvider(HashFactProvider.class);
		bind(Project.class).toProvider(ProjectProvider.class);
		bind(Location.class).toProvider(LocationProvider.class);
	}

}
