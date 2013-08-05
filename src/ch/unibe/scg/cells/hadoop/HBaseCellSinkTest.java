package ch.unibe.scg.cells.hadoop;

import java.io.IOException;

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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
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
		try (HBaseCellSink<Void> cellSink =
				Guice.createInjector(new HadoopModule(), new TestModule()).getInstance(HBaseCellSink.class)) {
			ByteString key = ByteString.copyFromUtf8("123");
			// TODO check that the table is empty.
			cellSink.write(Cell.<Void>make(key, key, ByteString.EMPTY));
		}
		// TODO check that the row is not empty.
	}
}
