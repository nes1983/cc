package ch.unibe.scg.cc;

import javax.inject.Inject;
import javax.inject.Provider;

import ch.unibe.scg.cells.CellSink;
import ch.unibe.scg.cells.Codec;
import ch.unibe.scg.cells.Codecs;
import ch.unibe.scg.cells.Sink;

class SinkProvider<T, S extends Codec<T>> implements Provider<Sink<T>> {
	final private Provider<CellSink<T>> sink;
	final private Provider<? extends Codec<T>> codec;

	@Inject
	SinkProvider(Provider<CellSink<T>> sink, Provider<S> codec) {
		this.codec = codec;
		this.sink = sink;
	}

	@Override
	public Sink<T> get() {
		return Codecs.encode(sink.get(), codec.get());
	}
}
