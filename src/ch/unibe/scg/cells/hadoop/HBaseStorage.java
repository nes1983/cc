package ch.unibe.scg.cells.hadoop;


import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.inject.Qualifier;

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
	/** The name of a table underlying a source or sink. */
	@Qualifier
	@Target({ FIELD, PARAMETER, METHOD })
	@Retention(RUNTIME)
	static @interface TableName {}

	/** The name of the column family for the cell sources and sinks */
	@Qualifier
	@Target({ FIELD, PARAMETER, METHOD })
	@Retention(RUNTIME)
	static @interface FamilyName {}

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
