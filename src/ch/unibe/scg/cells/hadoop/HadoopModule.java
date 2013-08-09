package ch.unibe.scg.cells.hadoop;

import org.apache.hadoop.hbase.client.HTable;

import ch.unibe.scg.cells.CellLookupTable;
import ch.unibe.scg.cells.CellSink;
import ch.unibe.scg.cells.CellSource;
import ch.unibe.scg.cells.StorageModule;
import ch.unibe.scg.cells.hadoop.Annotations.IndexFamily;
import ch.unibe.scg.cells.hadoop.Annotations.WriteToWalEnabled;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.protobuf.ByteString;

/** Bindings that expose the cell interfaces */
public class HadoopModule extends AbstractModule implements StorageModule {
	@Override
	protected void configure() {
		bind(new TypeLiteral<CellSink<Void>>() {}).to(new TypeLiteral<HBaseCellSink<Void>>() {});
		bind(new TypeLiteral<CellSource<Void>>() {}).to(new TypeLiteral<HBaseCellSource<Void>>() {});
		bind(new TypeLiteral<CellLookupTable<Void>>() {}).to(new TypeLiteral<HBaseCellLookupTable<Void>>() {});
		bindConstant().annotatedWith(WriteToWalEnabled.class).to(false);
		bind(HTable.class).toProvider(HTableProvider.class);
		bind(ByteString.class).annotatedWith(IndexFamily.class).toInstance(ByteString.copyFromUtf8("i"));
	}
}
