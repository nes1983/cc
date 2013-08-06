package ch.unibe.scg.cells.hadoop;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.unibe.scg.cc.mappers.ConfigurationProvider;
import ch.unibe.scg.cells.Annotations.FamilyName;
import ch.unibe.scg.cells.Annotations.TableName;
import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.CellLookupTable;
import ch.unibe.scg.cells.CellSink;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.protobuf.ByteString;

/** Testing {@link HBaseCellSink} */
public final class HBaseCellSinkTest {
	private static final String TEST_TABLE_NAME = "hbasetesttable";
	private static final ByteString FAMILY = ByteString.copyFromUtf8("d");

	private Configuration conf;

	private class TestModule extends AbstractModule {
		@Override
		protected void configure() {
			bind(Configuration.class).toInstance(conf);
			bindConstant().annotatedWith(TableName.class).to(TEST_TABLE_NAME);
			bind(ByteString.class).annotatedWith(FamilyName.class).toInstance(FAMILY);
		}
	}

	@SuppressWarnings("javadoc")
	@Before
	public void createTable() throws IOException {
		conf = Guice.createInjector().getInstance(ConfigurationProvider.class).get();

		try (HBaseAdmin admin = new HBaseAdmin(conf)) {
			HTableDescriptor td = new HTableDescriptor(TEST_TABLE_NAME);
			HColumnDescriptor family = new HColumnDescriptor(FAMILY.toByteArray());
			td.addFamily(family);
			if (admin.isTableAvailable(TEST_TABLE_NAME)) {
				if (admin.isTableEnabled(TEST_TABLE_NAME)) {
					admin.disableTable(TEST_TABLE_NAME);
				}
				admin.deleteTable(TEST_TABLE_NAME);
			}
			admin.createTable(td);
		}
	}

	@SuppressWarnings("javadoc")
	@After
	public void deleteTable() throws IOException {
		try (HBaseAdmin admin = new HBaseAdmin(conf)) {
			admin.disableTable(TEST_TABLE_NAME);
			admin.deleteTable(TEST_TABLE_NAME);
		}
	}

	/** Testing {@link HBaseCellSink#write(Cell)}. */
	@Test
	public void writeSmokeTest() throws IOException {
		Injector i = Guice.createInjector(new HadoopModule(), new TestModule());
		ByteString key = ByteString.copyFromUtf8("123");

		try (HBaseCellLookupTable<Void> lookup = i.getInstance(HBaseCellLookupTable.class)) {
			Iterable<Cell<Void>> rowBeforeWrite = lookup.readRow(key);
			assertTrue(rowBeforeWrite.toString(), Iterables.isEmpty(rowBeforeWrite));

			try (HBaseCellSink<Void> cellSink = i.getInstance(HBaseCellSink.class)) {
				cellSink.write(Cell.<Void> make(key, key, ByteString.EMPTY));
			}

			Iterable<Cell<Void>> rowAfterWrite = lookup.readRow(key);
			assertFalse(rowAfterWrite.toString(), Iterables.isEmpty(rowAfterWrite));
		}
	}

	@Test
	public void checkTimes() throws IOException {
		final Injector injector = Guice.createInjector(new HadoopModule(), new TestModule());
		final ByteString key = ByteString.copyFromUtf8("123");
		final int rounds = 500;

		final Stopwatch writeStopWatch = new Stopwatch();
		try (CellSink<Void> sink = injector.getInstance(Key.get(new TypeLiteral<CellSink<Void>>() {}))) {
			writeStopWatch.start();
			for (int i = 0; i < rounds; i++) {
				sink.write(Cell.<Void> make(key, key, ByteString.EMPTY));
			}
		}
		writeStopWatch.stop();
		final long time2Write = writeStopWatch.elapsed(TimeUnit.MILLISECONDS);

		final Stopwatch readStopWatch = new Stopwatch();
		try (CellLookupTable<Void> lookup = injector.getInstance(HBaseCellLookupTable.class)) {
			readStopWatch.start();
			for (int i = 0; i < rounds; i++) {
				assertFalse(String.valueOf(i), Iterables.isEmpty(lookup.readRow(key)));
			}
		}
		readStopWatch.stop();
		final long time2Read = readStopWatch.elapsed(TimeUnit.MILLISECONDS);
		assertTrue(time2Write < time2Read);
	}
}
