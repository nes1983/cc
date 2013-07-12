package ch.unibe.scg.cc;

import javax.inject.Singleton;

import ch.unibe.scg.cc.Annotations.Function2Snippets;
import ch.unibe.scg.cc.Annotations.Populator;
import ch.unibe.scg.cc.Protos.CodeFile;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Protos.Project;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.Protos.Version;

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

		bind(projectShuffler).annotatedWith(Populator.class).to(projectShuffler).in(Singleton.class);
		bind(versionShuffler).annotatedWith(Populator.class).to(versionShuffler).in(Singleton.class);
		bind(codeFileShuffler).annotatedWith(Populator.class).to(codeFileShuffler).in(Singleton.class);
		bind(functionShuffler).annotatedWith(Populator.class).to(functionShuffler).in(Singleton.class);
		bind(snippetShuffler).annotatedWith(Populator.class).to(snippetShuffler).in(Singleton.class);
		bind(snippetShuffler).annotatedWith(Function2Snippets.class).to(snippetShuffler).in(Singleton.class);

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

		bind(new TypeLiteral<CellSink<Snippet>>() {}).annotatedWith(Function2Snippets.class)
			.to(Key.get(snippetShuffler, Function2Snippets.class));
		bind(new TypeLiteral<CellSource<Snippet>>() {}).annotatedWith(Function2Snippets.class)
			.to(Key.get(snippetShuffler, Function2Snippets.class));
	}
}
