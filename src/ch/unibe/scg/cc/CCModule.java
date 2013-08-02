package ch.unibe.scg.cc;

import java.security.MessageDigest;

import ch.unibe.scg.cc.Annotations.PopularSnippets;
import ch.unibe.scg.cc.Annotations.PopularSnippetsThreshold;
import ch.unibe.scg.cc.Annotations.Type2;
import ch.unibe.scg.cc.Protos.CodeFile;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Protos.Project;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.Protos.Version;
import ch.unibe.scg.cc.regex.Replace;
import ch.unibe.scg.cells.LookupTable;
import ch.unibe.scg.cells.Sink;
import ch.unibe.scg.cells.Source;

import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

//@formatter:off
public class CCModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(MessageDigest.class).toProvider(MessageDigestProvider.class);

		bind(new TypeLiteral<LookupTable<Str<Function>>>() {})
				.toProvider(new TypeLiteral<LookupTableProvider<Str<Function>, FunctionStringCodec>>() {});
		bind(new TypeLiteral<LookupTable<Function>>() {})
				.toProvider(new TypeLiteral<LookupTableProvider<Function, PopulatorCodec.FunctionCodec>>() {});
		bind(new TypeLiteral<LookupTable<CodeFile>>() {})
				.toProvider(new TypeLiteral<LookupTableProvider<CodeFile, PopulatorCodec.CodeFileCodec>>() {});
		bind(new TypeLiteral<LookupTable<Version>>() {})
				.toProvider(new TypeLiteral<LookupTableProvider<Version, PopulatorCodec.VersionCodec>>() {});
		bind(new TypeLiteral<LookupTable<Project>>() {})
				.toProvider(new TypeLiteral<LookupTableProvider<Project, PopulatorCodec.ProjectCodec>>() {});

		bind(new TypeLiteral<Sink<Project>>() {})
				.toProvider(new TypeLiteral<SinkProvider<Project, PopulatorCodec.ProjectCodec>>() {});
		bind(new TypeLiteral<Sink<Version>>() {})
				.toProvider(new TypeLiteral<SinkProvider<Version, PopulatorCodec.VersionCodec>>() {});
		bind(new TypeLiteral<Sink<CodeFile>>() {})
				.toProvider(new TypeLiteral<SinkProvider<CodeFile, PopulatorCodec.CodeFileCodec>>() {});
		bind(new TypeLiteral<Sink<Function>>() {})
				.toProvider(new TypeLiteral<SinkProvider<Function, PopulatorCodec.FunctionCodec>>() {});
		bind(new TypeLiteral<Sink<Snippet>>() {})
				.toProvider(new TypeLiteral<SinkProvider<Snippet, PopulatorCodec.Function2SnippetCodec>>() {});
		bind(new TypeLiteral<Sink<Str<Function>>>() {})
				.toProvider(new TypeLiteral<SinkProvider<Str<Function>, FunctionStringCodec>>() {});
		
		bind(new TypeLiteral<Source<Snippet>>() {}).annotatedWith(PopularSnippets.class)
				.toProvider(new TypeLiteral<SourceProvider<Snippet, PopularSnippetsCodec>>() {});
		bind(PopularSnippetMaps.class).toProvider(PopularSnippetMapsProvider.class);

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
