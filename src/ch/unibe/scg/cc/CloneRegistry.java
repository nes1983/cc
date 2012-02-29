package ch.unibe.scg.cc;

import java.sql.SQLException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.HashFact;
import ch.unibe.scg.cc.activerecord.Location;
import ch.unibe.scg.cc.activerecord.Project;

@Singleton
public class CloneRegistry {
	
	final Provider<HashFact> hashFactProvider;
	
	final Provider<Location> locationProvider;
	
	@Inject
	public CloneRegistry(Provider<HashFact> hashFactProvider, Provider<Location> locationProvider) {
		this.hashFactProvider = hashFactProvider;
		this.locationProvider = locationProvider;
	}
	
	public void register(byte[] hash, Project project, Function function,
			Location location, int type) {
		HashFact fact =  hashFactProvider.get();
		fact.setHash(hash);
		fact.setLocation(location);
		fact.setProject(project);
		fact.setFunction(function);
		fact.setType(type);
		fact.save();

	}



	public void register(byte[] hash, Project project, Function function,
			int from, int length, int type) {
		Location location = locationProvider.get();
		location.setFirstLine(from);
		this.register(hash, project, function, location, type);
	}


}
