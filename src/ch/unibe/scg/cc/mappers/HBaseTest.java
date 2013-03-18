package ch.unibe.scg.cc.mappers;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import ch.unibe.scg.cc.activerecord.ConfigurationProvider;
import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;
import com.google.inject.Injector;

@Ignore
public class HBaseTest {
	private static final String TEST_TABLE_NAME = "hbasetesttable";
	private Configuration conf;

	public HBaseTest() throws MasterNotRunningException, ZooKeeperConnectionException {
		Injector i = Guice.createInjector(new CCModule(), new JavaModule());
		conf = i.getInstance(ConfigurationProvider.class).get();
	}

	@Before
	public void createTable() throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor td = new HTableDescriptor(TEST_TABLE_NAME);
		HColumnDescriptor family = new HColumnDescriptor(GuiceResource.FAMILY);
		td.addFamily(family);
		admin.createTable(td);
		admin.close();
	}

	@Test
	public void checkGetReturn() throws IOException {
		HTable testTable = new HTable(conf, TEST_TABLE_NAME);
		byte[] key = new byte[] { 123 };
		Get get = new Get(key);
		assertTrue(testTable.get(get).isEmpty());
		Put put = new Put(key);
		put.add(GuiceResource.FAMILY, key, 0l, key);
		testTable.put(put);
		testTable.flushCommits();
		assertFalse(testTable.get(get).isEmpty());
		testTable.close();
	}

	@After
	public void deleteTable() throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable(TEST_TABLE_NAME);
		admin.deleteTable(TEST_TABLE_NAME);
		admin.close();
	}
}
