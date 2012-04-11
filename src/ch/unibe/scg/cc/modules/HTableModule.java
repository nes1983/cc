package ch.unibe.scg.cc.modules;

import javax.inject.Singleton;

import org.apache.hadoop.hbase.client.HTable;

import ch.unibe.scg.cc.activerecord.HTableProvider;

import com.google.inject.PrivateModule;
import com.google.inject.name.Names;


public abstract class HTableModule extends PrivateModule {
	private final String named;

	HTableModule(String named) {
		this.named = named;
	}

	@Override
	protected void configure() {
		bind(HTable.class).annotatedWith(Names.named(named))
				.toProvider(HTableProvider.class).in(Singleton.class);
		expose(HTable.class).annotatedWith(Names.named(named));

		bindHTable();
	}

	abstract void bindHTable();
}