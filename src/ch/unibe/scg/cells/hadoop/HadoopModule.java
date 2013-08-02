package ch.unibe.scg.cells.hadoop;

import ch.unibe.scg.cells.CellSink;
import ch.unibe.scg.cells.hadoop.Annotations.WriteToWalEnabled;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

/** Bindings that expose the cell interfaces */
public class HadoopModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(new TypeLiteral<CellSink<?>>() {}).to(new TypeLiteral<HBaseCellSink<?>>() {});
		bindConstant().annotatedWith(WriteToWalEnabled.class).to(false);
	}
}
