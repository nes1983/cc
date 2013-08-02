package ch.unibe.scg.cc;

import javax.inject.Inject;
import javax.inject.Provider;

import ch.unibe.scg.cells.CellLookupTable;
import ch.unibe.scg.cells.Codec;
import ch.unibe.scg.cells.Codecs;
import ch.unibe.scg.cells.LookupTable;

class LookupTableProvider<T, S extends Codec<T>> implements Provider<LookupTable<T>> {
	final private Provider<CellLookupTable<T>> cellTable;
	final private Provider<? extends Codec<T>> codec;

	@Inject
	LookupTableProvider(Provider<CellLookupTable<T>> cellTable, Provider<S> codec) {
		this.cellTable = cellTable;
		this.codec = codec;
	}

	@Override
	public LookupTable<T> get() {
		return Codecs.decodedTable(cellTable.get(), codec.get());
	}
}
