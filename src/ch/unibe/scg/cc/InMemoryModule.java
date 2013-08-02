package ch.unibe.scg.cc;

import javax.inject.Singleton;

import ch.unibe.scg.cc.Annotations.PopularSnippets;
import ch.unibe.scg.cc.Annotations.Populator;
import ch.unibe.scg.cc.Annotations.Snippet2Functions;
import ch.unibe.scg.cc.Protos.CodeFile;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Protos.Project;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.Protos.Version;
import ch.unibe.scg.cells.CellLookupTable;
import ch.unibe.scg.cells.CellSink;
import ch.unibe.scg.cells.CellSource;
import ch.unibe.scg.cells.InMemoryShuffler;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

class InMemoryModule extends AbstractModule {
	@Override
	public void configure() {
		TypeLiteral<InMemoryShuffler<Project>> projectShuffler = new TypeLiteral<InMemoryShuffler<Project>>() {};
		TypeLiteral<InMemoryShuffler<Version>> versionShuffler = new TypeLiteral<InMemoryShuffler<Version>>() {};
		TypeLiteral<InMemoryShuffler<CodeFile>> codeFileShuffler = new TypeLiteral<InMemoryShuffler<CodeFile>>() {};
		TypeLiteral<InMemoryShuffler<Function>> functionShuffler = new TypeLiteral<InMemoryShuffler<Function>>() {};
		TypeLiteral<InMemoryShuffler<Snippet>> snippetShuffler = new TypeLiteral<InMemoryShuffler<Snippet>>() {};
		TypeLiteral<InMemoryShuffler<Str<Function>>> functionStrShuffler
				= new TypeLiteral<InMemoryShuffler<Str<Function>>>() {};

		// These must all be singletons, to make sure that the shuffler can serve as both sink and source.
		bind(Key.get(projectShuffler, Populator.class)).to(projectShuffler).in(Singleton.class);
		bind(Key.get(versionShuffler, Populator.class)).to(versionShuffler).in(Singleton.class);
		bind(Key.get(codeFileShuffler, Populator.class)).to(codeFileShuffler).in(Singleton.class);
		bind(Key.get(functionShuffler, Populator.class)).to(functionShuffler).in(Singleton.class);
		bind(Key.get(snippetShuffler, Populator.class)).to(snippetShuffler).in(Singleton.class);
		bind(Key.get(snippetShuffler, Snippet2Functions.class)).to(snippetShuffler).in(Singleton.class);
		bind(Key.get(snippetShuffler, PopularSnippets.class)).to(snippetShuffler).in(Singleton.class);
		bind(Key.get(functionStrShuffler)).in(Singleton.class);

		bind(new TypeLiteral<CellSink<Project>>() {}).to(Key.get(projectShuffler, Populator.class));
		bind(new TypeLiteral<CellSource<Project>>() {}).to(Key.get(projectShuffler, Populator.class));
		bind(new TypeLiteral<CellSink<Version>>() {}).to(Key.get(versionShuffler, Populator.class));
		bind(new TypeLiteral<CellSource<Version>>() {}).to(Key.get(versionShuffler, Populator.class));
		bind(new TypeLiteral<CellSink<CodeFile>>() {}).to(Key.get(codeFileShuffler, Populator.class));
		bind(new TypeLiteral<CellSource<CodeFile>>() {}).to(Key.get(codeFileShuffler, Populator.class));
		bind(new TypeLiteral<CellSink<Function>>() {}).to(Key.get(functionShuffler, Populator.class));
		bind(new TypeLiteral<CellSource<Function>>() {}).to(Key.get(functionShuffler, Populator.class));
		bind(new TypeLiteral<CellSink<Snippet>>() {}).to(Key.get(snippetShuffler, Populator.class));
		bind(new TypeLiteral<CellSource<Snippet>>() {}).to(Key.get(snippetShuffler, Populator.class));
		bind(new TypeLiteral<CellSink<Str<Function>>>() {}).to(functionStrShuffler);
		bind(new TypeLiteral<CellSource<Str<Function>>>() {}).to(functionStrShuffler);

		bind(new TypeLiteral<CellSink<Snippet>>() {}).annotatedWith(Snippet2Functions.class)
			.to(Key.get(snippetShuffler, Snippet2Functions.class));
		bind(new TypeLiteral<CellSource<Snippet>>() {}).annotatedWith(Snippet2Functions.class)
			.to(Key.get(snippetShuffler, Snippet2Functions.class));
		bind(new TypeLiteral<CellSink<Snippet>>() {}).annotatedWith(PopularSnippets.class)
			.to(Key.get(snippetShuffler, PopularSnippets.class));
		bind(new TypeLiteral<CellSource<Snippet>>() {}).annotatedWith(PopularSnippets.class)
			.to(Key.get(snippetShuffler, PopularSnippets.class));

		bind(new TypeLiteral<CellLookupTable<Str<Function>>>() {}).to(functionStrShuffler);
		bind(new TypeLiteral<CellLookupTable<Function>>() {}).to(Key.get(functionShuffler, Populator.class));
		bind(new TypeLiteral<CellLookupTable<CodeFile>>() {}).to(Key.get(codeFileShuffler, Populator.class));
		bind(new TypeLiteral<CellLookupTable<Version>>() {}).to(Key.get(versionShuffler, Populator.class));
		bind(new TypeLiteral<CellLookupTable<Project>>() {}).to(Key.get(projectShuffler, Populator.class));
	}
}
