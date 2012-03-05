package ch.unibe.scg.cc.activerecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.inject.Provider;

public class ConfigurationProvider implements Provider<Configuration> {

	@Override
	public Configuration get() {
		return HBaseConfiguration.create();
	}

}
