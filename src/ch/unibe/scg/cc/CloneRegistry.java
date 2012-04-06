package ch.unibe.scg.cc;

import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.HashFact;
import ch.unibe.scg.cc.activerecord.Location;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.RealCodeFile;
import ch.unibe.scg.cc.activerecord.Version;

@Singleton
public class CloneRegistry {

	public static final int TABLE_PROJECTS = 1;
	public static final int TABLE_CODEFILES = 2;
	public static final int TABLE_FUNCTIONS = 3;
	public static final int TABLE_VERSIONS = 4;
	public static final int TABLE_STRINGS = 5;
	
	final Provider<HashFact> hashFactProvider;
	
	final Provider<Location> locationProvider;

	@Inject @Named("projects")
	HTable projects;

	@Inject @Named("versions")
	HTable versions;
	
	@Inject @Named("files")
	HTable codefiles;
	
	@Inject @Named("functions")
	HTable functions;
	
	@Inject @Named("strings")
	HTable strings;
	
	@Inject
	public CloneRegistry(Provider<HashFact> hashFactProvider, Provider<Location> locationProvider) {
		this.hashFactProvider = hashFactProvider;
		this.locationProvider = locationProvider;
	}
	
	public void register(byte[] hash, Function function, Location location, int type) {
		HashFact fact = hashFactProvider.get();
		fact.setHash(hash);
		fact.setLocation(location);
		fact.setFunction(function);
		fact.setType(type);
		function.addHashFact(fact);
	}

	public void register(byte[] hash, Function function,
			int from, int length, int type) {
		Location location = locationProvider.get();
		location.setFirstLine(from);
		this.register(hash, function, location, type);
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
	
	public boolean lookupCodeFile(byte[] key) {
		HTable t = getTable(CloneRegistry.TABLE_CODEFILES);
		assert t != null;
		byte[] startKey = Bytes.add(key, new byte[] {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		byte[] endKey = Bytes.add(key, new byte[] {127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127,127});
		Scan s = new Scan(startKey, endKey); // XXX funkt. das?
		try {
			return t.getScanner(s).next() != null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private HTable getTable(int table) {
		switch(table) {
		case TABLE_PROJECTS : return projects;
		case TABLE_CODEFILES : return codefiles;
		case TABLE_FUNCTIONS : return functions;
		case TABLE_VERSIONS : return versions;
		case TABLE_STRINGS : return strings;
		default: return null;
		}
	}

	public void register(CodeFile codeFile) {
		for(Function function : codeFile.getFunctions()) {
			Put put = new Put(Bytes.add(codeFile.getFileContentsHash(), function.getHash()));
			try {
				put.add(Bytes.toBytes(RealCodeFile.FAMILY_NAME),
						Bytes.toBytes(RealCodeFile.FUNCTION_OFFSET_NAME), 0l,
						Bytes.toBytes(function.getBaseLine()));
				codefiles.put(put);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * saves the project in the projects table
	 * @param project
	 */
	public void register(Project project) {
		Put put = new Put(project.getHash());
		try {
			project.save(put);
			projects.put(put);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * saves the version in the versions table
	 * @param version
	 */
	public void register(Version version) {
		Put put = new Put(version.getHash());
		try {
			version.save(put);
			versions.put(put);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void register(Function function) {
		for(HashFact hashFact : function.getHashFacts()) {
			Put put = new Put(Bytes.add(function.getHash(), Bytes.toBytes(hashFact.getType()), hashFact.getHash()));
			try {
				hashFact.save(put);
				functions.put(put);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

}
