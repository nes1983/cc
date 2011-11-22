package ch.unibe.scg.cc.modules;

import javax.inject.Provider;

import ch.unibe.scg.cc.Normalizer;
import ch.unibe.scg.cc.PhaseFrontent;
import ch.unibe.scg.cc.Tokenizer;
import ch.unibe.scg.cc.*;
import ch.unibe.scg.cc.Type2ReplacerFactory;
import ch.unibe.scg.cc.javaFrontend.JavaTokenizer;
import ch.unibe.scg.cc.javaFrontend.JavaType1ReplacerFactory;
import ch.unibe.scg.cc.regex.*;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

public class JavaModule extends AbstractModule{
	
	@Override
	protected void configure() {
		bind(Tokenizer.class).to(JavaTokenizer.class);
		install(new Type1Module());
		install(new Type2Module());
		bind(Frontend.class).annotatedWith(Java.class).to(Frontend.class); //XXX Make private
	}
}


class Type1Module extends PrivateModule {
	@Override
	protected void configure() {
		bind(PhaseFrontent.class).annotatedWith(Type1.class).to(Normalizer.class);
		expose(PhaseFrontent.class).annotatedWith(Type1.class);
		
		//Private:
		bind(new TypeLiteral<Replace[]>() {}).toProvider(JavaType1ReplacerFactory.class);
	}
}

class Type2Module extends PrivateModule {
	@Override
	protected void configure() {
		bind(PhaseFrontent.class).annotatedWith(Type2.class).to(Normalizer.class);
		expose(PhaseFrontent.class).annotatedWith(Type2.class);
		
		//Private:
		bind(new TypeLiteral<Replace[]>() {}).toProvider(Type2ReplacerFactory.class);
	}
}