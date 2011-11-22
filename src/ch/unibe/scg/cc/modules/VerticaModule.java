package ch.unibe.scg.cc.modules;

import java.sql.Connection;

import ch.unibe.scg.cc.VerticaConnectionProvider;

import com.google.inject.AbstractModule;

public class VerticaModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(Connection.class).toProvider(VerticaConnectionProvider.class);
	}

}
