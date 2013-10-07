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
import ch.unibe.scg.cells.TableModule.DefaultTableModule;

import com.google.inject.PrivateModule;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.protobuf.ByteString;

// TODO: Change to a PrivateModule to control visibility.
/** Configuration that gets all of the CC package configured. */
public final class CCModule extends CellsModule {
	final private StorageModule storageModule;

	/** HBase and InMemory both work as storageModules. */
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

		installTable(Populator.class, new TypeLiteral<Snippet>() {}, PopulatorCodec.Function2SnippetCodec.class,
				storageModule, new DefaultTableModule("Snippets", defaultFamily));
		installTable(Populator.class, new TypeLiteral<Function>() {}, PopulatorCodec.FunctionCodec.class,
				storageModule, new DefaultTableModule("Functions", defaultFamily));
		installTable(Populator.class, new TypeLiteral<CodeFile>() {}, PopulatorCodec.CodeFileCodec.class,
				storageModule, new DefaultTableModule("CodeFiles", defaultFamily));
		installTable(Populator.class, new TypeLiteral<Version>() {}, PopulatorCodec.VersionCodec.class,
				storageModule, new DefaultTableModule("Versions", defaultFamily));
		installTable(Populator.class, new TypeLiteral<Project>() {}, PopulatorCodec.ProjectCodec.class,
				storageModule, new DefaultTableModule("Projects", defaultFamily));
		installTable(Populator.class, new TypeLiteral<Str<Function>>() {}, FunctionStringCodec.class,
				storageModule, new DefaultTableModule("FunctionStrings", defaultFamily));
		installTable(PopularSnippets.class, new TypeLiteral<Snippet>() {}, PopularSnippetsCodec.class,
				storageModule, new DefaultTableModule("PopularSnippets", defaultFamily));

		bind(PopularSnippetMaps.class).in(Singleton.class);

		bindConstant().annotatedWith(PopularSnippetsThreshold.class).to(500);

		install(new Type2Module());
	}
}
