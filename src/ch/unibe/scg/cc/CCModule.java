package ch.unibe.scg.cc;

import java.security.MessageDigest;

import ch.unibe.scg.cc.Annotations.Function2RoughClones;
import ch.unibe.scg.cc.Annotations.Function2Snippets;
import ch.unibe.scg.cc.Annotations.PopularSnippets;
import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.Snippet;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

//@formatter:off
public class CCModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(MessageDigest.class).toProvider(MessageDigestProvider.class);

		bind(new TypeLiteral<Codec<Clone>>() {}).annotatedWith(Function2RoughClones.class)
				.to(Function2RoughClonesCodec.class);
		bind(new TypeLiteral<Codec<Snippet>>() {}).annotatedWith(PopularSnippets.class)
				.to(PopularSnippetsCodec.class);
		bind(new TypeLiteral<Codec<Snippet>>() {}).annotatedWith(Function2Snippets.class)
				.to(Function2SnippetsCodec.class);
	}
}
