package ch.unibe.scg.cc.javaFrontend;

import ch.unibe.scg.cc.Annotations.Java;
import ch.unibe.scg.cc.Annotations.Type1;
import ch.unibe.scg.cc.Normalizer;
import ch.unibe.scg.cc.Populator;
import ch.unibe.scg.cc.ReplacerNormalizer;
import ch.unibe.scg.cc.Tokenizer;
import ch.unibe.scg.cc.regex.Replace;

import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

/** Bindings for a normalizer in the Java programming language. */
public class JavaModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(Tokenizer.class).to(JavaTokenizer.class);
		install(new Type1Module());
		bind(Populator.class).annotatedWith(Java.class).to(Populator.class); // TODO: Make private																			// Make
	}
}

class Type1Module extends PrivateModule {
	@Override
	protected void configure() {
		bind(Normalizer.class).annotatedWith(Type1.class).to(ReplacerNormalizer.class);
		expose(Normalizer.class).annotatedWith(Type1.class);

		// Private:
		bind(new TypeLiteral<Replace[]>() {}).toProvider(JavaType1ReplacerFactory.class);
	}
}
