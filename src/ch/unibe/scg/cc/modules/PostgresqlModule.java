package ch.unibe.scg.cc.modules;

import java.sql.Connection;

import ch.unibe.scg.cc.PostgresqlConnectionProvider;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.FunctionProvider;
import ch.unibe.scg.cc.activerecord.HashFact;
import ch.unibe.scg.cc.activerecord.HashFactProvider;
import ch.unibe.scg.cc.activerecord.Location;
import ch.unibe.scg.cc.activerecord.LocationProvider;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.ProjectProvider;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

public class PostgresqlModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(Connection.class).toProvider(PostgresqlConnectionProvider.class).in(Scopes.SINGLETON);

	}

}