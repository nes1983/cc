package ch.unibe.scg.cc.activerecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.inject.Provider;

public class ConfigurationProvider implements Provider<Configuration> {

	public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM     = "hbase.zookeeper.quorum";
	public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
	
	@Override
	public Configuration get() {
		Configuration hConf = HBaseConfiguration.create();
		hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "pinocchio.unibe.ch");
		hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, "2181");
		return hConf;
	}
}
