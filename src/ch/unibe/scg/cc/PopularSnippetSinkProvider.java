package ch.unibe.scg.cc;

import javax.inject.Inject;
import javax.inject.Provider;

import ch.unibe.scg.cc.Annotations.PopularSnippets;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cells.CellSink;
import ch.unibe.scg.cells.Codec;
import ch.unibe.scg.cells.Codecs;
import ch.unibe.scg.cells.Sink;

/** 
 * This class is very similar to SinkProvider.java. 
 * Sadly, there is no way to inject into SinkProvider so that we get a PopularSnippetCellSink. 
 */
class PopularSnippetSinkProvider implements Provider<Sink<Snippet>> {
	final private Provider<CellSink<Snippet>> sink;
	final private Provider<? extends Codec<Snippet>> codec;

	@Inject
	PopularSnippetSinkProvider(@PopularSnippets Provider<CellSink<Snippet>> sink, Provider<PopularSnippetsCodec> codec) {
		this.codec = codec;
		this.sink = sink;
	}

	@Override
	public Sink<Snippet> get() {
		return Codecs.encode(sink.get(), codec.get());
	}
}
