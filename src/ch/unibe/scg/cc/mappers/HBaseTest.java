package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;
import com.google.inject.Injector;

@Ignore
public class HBaseTest {

	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

	@Test
	@Before
	public void createTable() throws IOException {
		Injector i = Guice.createInjector(new CCModule(), new JavaModule());
		HBaseConfiguration conf = i.getInstance(HBaseConfiguration.class);
		conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "leela.unibe.ch");
		conf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, "2181");

		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor td = new HTableDescriptor("asdf");
		admin.createTable(td);
	}

	@Test
	@After
	public void deleteTable() throws IOException {
		Injector i = Guice.createInjector(new CCModule(), new JavaModule());
		HBaseConfiguration conf = i.getInstance(HBaseConfiguration.class);

		HBaseAdmin admin = new HBaseAdmin(conf);
		conf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "leela.unibe.ch");
		conf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, "2181");
		admin.disableTable("asdf");
		admin.deleteTable("asdf");
	}
}
