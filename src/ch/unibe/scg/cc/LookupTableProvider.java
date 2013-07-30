package ch.unibe.scg.cc;

import javax.inject.Inject;
import javax.inject.Provider;

import org.unibe.scg.cells.CellLookupTable;
import org.unibe.scg.cells.Codec;
import org.unibe.scg.cells.Codecs;
import org.unibe.scg.cells.LookupTable;

class LookupTableProvider<T> implements Provider<LookupTable<T>> {
	final private Provider<CellLookupTable<T>> cellTable;
	final private Provider<Codec<T>> codec;

	@Inject
	LookupTableProvider(Provider<CellLookupTable<T>> cellTable, Provider<Codec<T>> codec) {
		this.cellTable = cellTable;
		this.codec = codec;
	}

	@Override
	public LookupTable<T> get() {
		return Codecs.decodedTable(cellTable.get(), codec.get());
	}
}
