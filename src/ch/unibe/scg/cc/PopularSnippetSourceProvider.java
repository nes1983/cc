package ch.unibe.scg.cc;

import javax.inject.Inject;
import javax.inject.Provider;

import org.unibe.scg.cells.CellSource;
import org.unibe.scg.cells.Codec;
import org.unibe.scg.cells.Codecs;
import org.unibe.scg.cells.Source;

import ch.unibe.scg.cc.Annotations.PopularSnippets;
import ch.unibe.scg.cc.Protos.Snippet;

// TODO: This class is ridiculously specific for popular snippets.
// As soon as this is necessary again, build using private modules.
class PopularSnippetSourceProvider implements Provider<Source<Snippet>> {
	final private Provider<CellSource<Snippet>> popularCells;
	final private Provider<Codec<Snippet>> popularCodec;

	@Inject
	PopularSnippetSourceProvider(@PopularSnippets Provider<CellSource<Snippet>> popularCells,
			@PopularSnippets Provider<Codec<Snippet>> popularCodec) {
		this.popularCells = popularCells;
		this.popularCodec = popularCodec;
	}

	@Override
	public Source<Snippet> get() {
		return Codecs.decode(popularCells.get(), popularCodec.get());
	}
}
