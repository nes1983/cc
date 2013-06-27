package ch.unibe.scg.cc.mappers;

import org.apache.hadoop.hbase.client.Scan;

import ch.unibe.scg.cc.mappers.HTableWriteBuffer.BufferFactory;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;

public class MapperModule extends AbstractModule {
	@Override
	protected void configure() {
		install(new FactoryModuleBuilder().build(BufferFactory.class));
		bind(Scan.class).toProvider(ScanProvider.class);
	}
}
