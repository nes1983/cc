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

/** Bindings for CellSource<Void>, CellSink<Void>, CellLookupTable<Void> to their HBase implementations. */
public class HBaseStorage extends AbstractModule implements StorageModule {
	/** The column family for indexes. */
	static ByteString INDEX_FAMILY = ByteString.copyFromUtf8("i");

	@SuppressWarnings("unchecked") // Class literals just cannot be parameterized.
	@Override
	protected void configure() {
		bind(ByteString.class).annotatedWith(IndexFamily.class).toInstance(INDEX_FAMILY);
		bindConstant().annotatedWith(WriteToWalEnabled.class).to(false);

		bind(new TypeLiteral<CellSource<Void>>() {}).to((Class<? extends CellSource<Void>>) HBaseCellSource.class);
		bind(new TypeLiteral<CellSink<Void>>() {}).to((Class<? extends CellSink<Void>>) HBaseCellSink.class);
		bind(new TypeLiteral<CellLookupTable<Void>>() {}).to((Class<? extends CellLookupTable<Void>>) HBaseCellLookupTable.class);
		bind(HTable.class).toProvider(HTableProvider.class);
	}
}
