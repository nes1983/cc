package ch.unibe.scg.cc.mappers;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import ch.unibe.scg.cc.CCModule;
import ch.unibe.scg.cc.activerecord.ConfigurationProvider;
import ch.unibe.scg.cc.javaFrontend.JavaModule;

import com.google.common.base.Stopwatch;
import com.google.inject.Guice;
import com.google.inject.Injector;

@SuppressWarnings("javadoc")
@Ignore // Modifies the database.
public class HBaseTest {
	private static final String TEST_TABLE_NAME = "hbasetesttable";
	private final Configuration conf;

	public HBaseTest() {
		Injector i = Guice.createInjector(new CCModule(), new JavaModule());
		conf = i.getInstance(ConfigurationProvider.class).get();
	}

	@Before
	public void createTable() throws IOException {
		try (HBaseAdmin admin = new HBaseAdmin(conf)) {
			HTableDescriptor td = new HTableDescriptor(TEST_TABLE_NAME);
			HColumnDescriptor family = new HColumnDescriptor(Constants.FAMILY);
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

	@Test
	public void checkGetReturn() throws IOException {
		try (HTable testTable = new HTable(conf, TEST_TABLE_NAME)) {
			byte[] key = new byte[] { 123 };
			Get get = new Get(key);
			assertTrue(testTable.get(get).isEmpty());
			Put put = new Put(key);
			put.add(Constants.FAMILY, key, 0l, key);
			testTable.put(put);
			testTable.flushCommits();
			assertFalse(testTable.get(get).isEmpty());
		}
	}

	@Test
	public void checkTimes() throws IOException {
		try (HTable testTable = new HTable(conf, TEST_TABLE_NAME)) {
			// settings adapted from HTableProvider
			testTable.setAutoFlush(false);
			testTable.setWriteBufferSize(1024 * 1024 * 12);
			testTable.getTableDescriptor().setDeferredLogFlush(true);

			final byte[] key = new byte[] { 123 };
			final int rounds = 500;
			final long time2Read, time2Write;

			Stopwatch stopwatch = new Stopwatch().start();
			for (int i = 0; i < rounds; i++) {
				Put put = new Put(key);
				put.setWriteToWAL(false);
				put.add(Constants.FAMILY, key, 0l, key);
				testTable.put(put);
			}
			testTable.flushCommits();
			stopwatch.stop();
			time2Write = stopwatch.elapsed(TimeUnit.MILLISECONDS);

			// not using caching on purpose, simulates random read on disk
			stopwatch.reset().start();
			for (int i = 0; i < rounds; i++) {
				Get get = new Get(key);
				if (testTable.get(get).isEmpty()) {
					Assert.fail("Key should exist here!");
				}
			}
			stopwatch.stop();
			time2Read = stopwatch.elapsed(TimeUnit.MILLISECONDS);
			assertTrue(time2Write < time2Read);
		}
	}

	@After
	public void deleteTable() throws IOException {
		try (HBaseAdmin admin = new HBaseAdmin(conf)) {
			admin.disableTable(TEST_TABLE_NAME);
			admin.deleteTable(TEST_TABLE_NAME);
		}
	}
}
