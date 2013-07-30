package ch.unibe.scg.cc;

import java.security.MessageDigest;

import org.unibe.scg.cells.Codec;
import org.unibe.scg.cells.LookupTable;
import org.unibe.scg.cells.Source;

import ch.unibe.scg.cc.Annotations.Function2FineClones;
import ch.unibe.scg.cc.Annotations.Function2RoughClones;
import ch.unibe.scg.cc.Annotations.PopularSnippets;
import ch.unibe.scg.cc.Annotations.PopularSnippetsThreshold;
import ch.unibe.scg.cc.Annotations.Snippet2Functions;
import ch.unibe.scg.cc.Annotations.Type2;
import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Protos.GitRepo;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.regex.Replace;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

//@formatter:off
public class CCModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(MessageDigest.class).toProvider(MessageDigestProvider.class);

		bind(new TypeLiteral<LookupTable<Str<Function>>>() {})
				.toProvider(new TypeLiteral<LookupTableProvider<Str<Function>>>() {});

		bind(new TypeLiteral<Source<Snippet>>() {}).annotatedWith(PopularSnippets.class)
				.toProvider(PopularSnippetSourceProvider.class);
		bind(PopularSnippetMaps.class).toProvider(PopularSnippetMapsProvider.class);

		bind(new TypeLiteral<Codec<Clone>>() {}).annotatedWith(Function2RoughClones.class)
				.to(Function2RoughClonesCodec.class);
		bind(new TypeLiteral<Codec<Snippet>>() {}).annotatedWith(PopularSnippets.class)
				.to(PopularSnippetsCodec.class);
		bind(new TypeLiteral<Codec<Snippet>>() {}).annotatedWith(Snippet2Functions.class)
				.to(Snippet2FunctionsCodec.class);
		bind(new TypeLiteral<Codec<Clone>>() {}).annotatedWith(Function2FineClones.class)
				.to(Function2FineClonesCodec.class);
		bind(new TypeLiteral<Codec<GitRepo>>() {}).to(GitRepoCodec.class);


		bind(Key.get(new TypeLiteral<Codec<Snippet>>() {}, Snippet2Functions.class)).to(Snippet2FunctionsCodec.class);
		bind(Key.get(new TypeLiteral<Codec<Clone>>() {}, Function2RoughClones.class)).to(Function2RoughClonesCodec.class);
		bind(Key.get(new TypeLiteral<Codec<Str<Function>>>() {})).to(FunctionStringCodec.class);

		bindConstant().annotatedWith(PopularSnippetsThreshold.class).to(500);

		install(new Type2Module());
	}

	private static class Type2Module extends PrivateModule {
		@Override
		protected void configure() {
			bind(Normalizer.class).annotatedWith(Type2.class).to(ReplacerNormalizer.class);
			expose(Normalizer.class).annotatedWith(Type2.class);

			// Private:
			bind(new TypeLiteral<Replace[]>() {}).toProvider(Type2ReplacerFactory.class);
		}
	}
}
