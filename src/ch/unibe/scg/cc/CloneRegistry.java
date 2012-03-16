package ch.unibe.scg.cc;

import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.HashFact;
import ch.unibe.scg.cc.activerecord.Location;
import ch.unibe.scg.cc.activerecord.Project;

@Singleton
public class CloneRegistry {

	public static final int TABLE_PROJECTS = 1;
	public static final int TABLE_CODEFILES = 2;
	public static final int TABLE_FUNCTIONS = 3;
	public static final int TABLE_FACTS = 4;
	public static final int TABLE_STRINGS = 5;
	
	final Provider<HashFact> hashFactProvider;
	
	final Provider<Location> locationProvider;
	
	@Inject @Named("projects")
	HTable projects;
	
	@Inject @Named("files")
	HTable codefiles;
	
	@Inject @Named("functions")
	HTable functions;
	
	@Inject @Named("facts")
	HTable facts;
	
	@Inject @Named("strings")
	HTable strings;
	
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

	/**
	 * looks up a key in the provided table
	 * @param key
	 * @param table
	 * @return true if the key was found, otherwise false
	 */
	public boolean lookup(byte[] key, int table) {
		HTable t = getTable(table);
		assert t != null;
		Get k = new Get(key);
		try {
			return !t.get(k).isEmpty();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private HTable getTable(int table) {
		switch(table) {
		case TABLE_PROJECTS : return projects;
		case TABLE_CODEFILES : return codefiles;
		case TABLE_FUNCTIONS : return functions;
		case TABLE_FACTS : return facts;
		case TABLE_STRINGS : return strings;
		default: return null;
		}
	}

	public void register(CodeFile codeFile) {
		Put put = new Put(codeFile.getHash());
		try {
			codeFile.save(put);
			codefiles.put(put);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void register(Project project) {
		Put put = new Put(project.getHash());
		try {
			project.save(put);
			projects.put(put);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
