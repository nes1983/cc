package ch.unibe.scg.cc;

import ch.unibe.scg.cc.Annotations.PopularSnippets;
import ch.unibe.scg.cc.Annotations.PopularSnippetsThreshold;
import ch.unibe.scg.cc.Annotations.Populator;
import ch.unibe.scg.cc.Annotations.Type2;
import ch.unibe.scg.cc.Protos.CodeFile;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Protos.Project;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.Protos.Version;
import ch.unibe.scg.cc.regex.Replace;
import ch.unibe.scg.cells.CellsModule;
import ch.unibe.scg.cells.StorageModule;

import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.protobuf.ByteString;

// TODO: Change to a PrivateModule to control visibility.
public final class CCModule extends CellsModule {
	final private StorageModule storageModule;

	public CCModule(StorageModule storageModule) {
		this.storageModule = storageModule;
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

	@Override
	protected void configure() {
		ByteString defaultFamily = ByteString.copyFromUtf8("f");
		installTable("Snippets", defaultFamily, Populator.class,
				new TypeLiteral<Snippet>() {},  PopulatorCodec.Function2SnippetCodec.class, storageModule);
		installTable("Functions", defaultFamily, Populator.class,
				new TypeLiteral<Function>() {}, PopulatorCodec.FunctionCodec.class, storageModule);
		installTable("CodeFiles", defaultFamily, Populator.class,
				new TypeLiteral<CodeFile>() {},  PopulatorCodec.CodeFileCodec.class, storageModule);
		installTable("Versions", defaultFamily, Populator.class,
				new TypeLiteral<Version>() {}, PopulatorCodec.VersionCodec.class, storageModule);
		installTable("Projects", defaultFamily, Populator.class,
				new TypeLiteral<Project>() {}, PopulatorCodec.ProjectCodec.class, storageModule);
		installTable("FunctionStrings", defaultFamily, Populator.class,
				new TypeLiteral<Str<Function>>() {}, FunctionStringCodec.class, storageModule);
		installTable("PopularSnippets", defaultFamily, PopularSnippets.class,
				new TypeLiteral<Snippet>() {}, PopularSnippetsCodec.class, storageModule);

		bind(PopularSnippetMaps.class).toProvider(PopularSnippetMapsProvider.class);

		bindConstant().annotatedWith(PopularSnippetsThreshold.class).to(500);

		install(new Type2Module());
	}
}
