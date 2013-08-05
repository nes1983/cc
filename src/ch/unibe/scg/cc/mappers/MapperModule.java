package ch.unibe.scg.cc.mappers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;

import ch.unibe.scg.cc.mappers.HTableWriteBuffer.BufferFactory;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

class MapperModule extends AbstractModule {
	@Override
	protected void configure() {
		install(new FactoryModuleBuilder().build(BufferFactory.class));
		bind(Scan.class).toProvider(ScanProvider.class);
		bind(Configuration.class).toProvider(ConfigurationProvider.class);
		bind(Boolean.class).annotatedWith(Names.named("writeToWalEnabled")).toInstance(Boolean.FALSE);
	}
}
