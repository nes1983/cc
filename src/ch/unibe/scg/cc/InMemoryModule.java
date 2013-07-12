package ch.unibe.scg.cc;

import javax.inject.Singleton;

import ch.unibe.scg.cc.Protos.CodeFile;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Protos.Project;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.Protos.Version;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

class InMemoryModule extends AbstractModule {
	@Override
	public void configure() {
		TypeLiteral<InMemoryShuffler<Project>> projectShuffler = new TypeLiteral<InMemoryShuffler<Project>>() {};
		TypeLiteral<InMemoryShuffler<Version>> versionShuffler = new TypeLiteral<InMemoryShuffler<Version>>() {};
		TypeLiteral<InMemoryShuffler<CodeFile>> codeFileShuffler = new TypeLiteral<InMemoryShuffler<CodeFile>>() {};
		TypeLiteral<InMemoryShuffler<Function>> functionShuffler = new TypeLiteral<InMemoryShuffler<Function>>() {};
		TypeLiteral<InMemoryShuffler<Snippet>> snippetShuffler = new TypeLiteral<InMemoryShuffler<Snippet>>() {};

		bind(projectShuffler).in(Singleton.class);
		bind(versionShuffler).in(Singleton.class);
		bind(codeFileShuffler).in(Singleton.class);
		bind(functionShuffler).in(Singleton.class);
		bind(snippetShuffler).in(Singleton.class);

		bind(new TypeLiteral<CellSink<Project>>() {}).to(projectShuffler);
		bind(new TypeLiteral<CellSource<Project>>() {}).to(projectShuffler);
		bind(new TypeLiteral<CellSink<Version>>() {}).to(versionShuffler);
		bind(new TypeLiteral<CellSource<Version>>() {}).to(versionShuffler);
		bind(new TypeLiteral<CellSink<CodeFile>>() {}).to(codeFileShuffler);
		bind(new TypeLiteral<CellSource<CodeFile>>() {}).to(codeFileShuffler);
		bind(new TypeLiteral<CellSink<Function>>() {}).to(functionShuffler);
		bind(new TypeLiteral<CellSource<Function>>() {}).to(functionShuffler);
		bind(new TypeLiteral<CellSink<Snippet>>() {}).to(snippetShuffler);
		bind(new TypeLiteral<CellSource<Snippet>>() {}).to(snippetShuffler);
	}
}
