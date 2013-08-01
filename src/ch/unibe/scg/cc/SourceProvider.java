package ch.unibe.scg.cc;

import javax.inject.Inject;
import javax.inject.Provider;

import org.unibe.scg.cells.CellSource;
import org.unibe.scg.cells.Codec;
import org.unibe.scg.cells.Codecs;
import org.unibe.scg.cells.Source;

class SourceProvider<T, S extends Codec<T>> implements Provider<Source<T>> {
	final private Provider<CellSource<T>> cellSrc;
	final private Provider<? extends Codec<T>> codec;

	@Inject
	SourceProvider(Provider<CellSource<T>> cellSrc, Provider<S> codec) {
		this.codec = codec;
		this.cellSrc = cellSrc;
	}

	@Override
	public Source<T> get() {
		return Codecs.decode(cellSrc.get(), codec.get());
	}
}
