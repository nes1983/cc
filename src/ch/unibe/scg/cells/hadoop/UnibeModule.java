package ch.unibe.scg.cells.hadoop;

import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.google.inject.AbstractModule;

class UnibeModule extends AbstractModule {
	static class UnibeConfigurationProvider implements Provider<Configuration> {
		@Override
		public Configuration get() {
			Configuration hConf = HBaseConfiguration.create();
			hConf.set("hbase.master", "leela.unibe.ch:60000");
			hConf.set("hbase.zookeeper.quorum", "leela.unibe.ch");
			hConf.setInt("hbase.zookeeper.property.clientPort", 2181);
			hConf.setBoolean("fs.automatic.close", false);
			return hConf;
		}
	}

	@Override
	protected void configure() {
		bind(Configuration.class).toProvider(UnibeConfigurationProvider.class);
	}
}