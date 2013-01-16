package ch.unibe.scg.cc;

import java.io.IOException;

import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Column;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.HashFact;
import ch.unibe.scg.cc.activerecord.Location;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.RealCodeFile;
import ch.unibe.scg.cc.activerecord.Version;

import com.google.inject.Inject;

@Singleton
public class CloneRegistry {

	public static final int TABLE_VERSIONS = 1;
	public static final int TABLE_FUNCTIONS = 2;
	public static final int TABLE_FACTS = 3;
	public static final int TABLE_FILES = 4;
	public static final int TABLE_STRINGS = 5;

	final Provider<HashFact> hashFactProvider;

	final Provider<Location> locationProvider;

	@Inject(optional = true)
	@Named("versions")
	HTable versions;

	@Inject(optional = true)
	@Named("files")
	HTable files;

	@Inject(optional = true)
	@Named("functions")
	HTable functions;

	@Inject(optional = true)
	@Named("facts")
	HTable facts;

	@Inject(optional = true)
	@Named("strings")
	HTable strings;

	@Inject(optional = true)
	@Named("hashfactContent")
	HTable hashfactContent;

	@Inject
	public CloneRegistry(Provider<HashFact> hashFactProvider, Provider<Location> locationProvider) {
		this.hashFactProvider = hashFactProvider;
		this.locationProvider = locationProvider;
	}

	public void register(byte[] hash, String snippet, Function function, Location location, byte type) {
		HashFact fact = hashFactProvider.get();
		fact.setHash(hash);
		fact.setSnippet(snippet);
		fact.setLocation(location);
		fact.setFunction(function);
		fact.setType(type);
		function.addHashFact(fact);
	}

	public void register(byte[] hash, String snippet, Function function, int from, int length, byte type) {
		Location location = locationProvider.get();
		location.setFirstLine(from);
		this.register(hash, snippet, function, location, type);
	}

	/**
	 * looks up a key in the provided table
	 * 
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
		HTable t = getTable(CloneRegistry.TABLE_FUNCTIONS);
		assert t != null;
		byte[] startKey = Bytes.add(key, new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
		byte[] endKey = Bytes.add(key, new byte[] { 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127,
				127, 127, 127, 127, 127, 127, 127 });
		Scan s = new Scan(startKey, endKey);
		try {
			return t.getScanner(s).next() != null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean lookupVersion(byte[] key) {
		HTable t = getTable(CloneRegistry.TABLE_FILES);
		assert t != null;
		byte[] startKey = Bytes.add(key, new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
		byte[] endKey = Bytes.add(key, new byte[] { 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127,
				127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127,
				127, 127, 127, 127, 127, 127, 127 });
		Scan s = new Scan(startKey, endKey);
		try {
			return t.getScanner(s).next() != null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean lookupProject(byte[] key) {
		HTable t = getTable(CloneRegistry.TABLE_VERSIONS);
		assert t != null;
		Scan s = new Scan(key, key);
		try {
			return t.getScanner(s).next() != null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private HTable getTable(int table) {
		switch (table) {
		case TABLE_VERSIONS:
			return versions;
		case TABLE_FUNCTIONS:
			return functions;
		case TABLE_FACTS:
			return facts;
		case TABLE_FILES:
			return files;
		case TABLE_STRINGS:
			return strings;
		default:
			return null;
		}
	}

	public void register(CodeFile codeFile) {
		byte[] hashFileContents = codeFile.getFileContentsHash();
		for (Function function : codeFile.getFunctions()) {
			Put put = new Put(hashFileContents);
			put.setWriteToWAL(false); // XXX performance increase
			try {
				put.add(RealCodeFile.FAMILY_NAME, function.getHash(), 0l, Bytes.toBytes(function.getBaseLine()));
				functions.put(put);

				Put fnSnippet = new Put(function.getHash()); // XXX
																// temp
																// debug
				fnSnippet.add(Column.FAMILY_NAME, Bytes.toBytes("fv"), 0l,
						Bytes.toBytes(function.getContents().toString())); // XXX
																			// temp
																			// debug
				hashfactContent.put(fnSnippet); // XXX temp debug
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * saves the project in the projects table
	 * 
	 * @param project
	 */
	public void register(Project project) {
		Put put = new Put(project.getHash());
		try {
			project.save(put);
			versions.put(put);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * saves the version in the versions table
	 * 
	 * @param version
	 */
	public void register(Version version) {
		Put put = new Put(version.getHash());
		try {
			version.save(put);
			files.put(put);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void register(Function function) {
		for (HashFact hashFact : function.getHashFacts()) {
			Put put = new Put(function.getHash());
			Put hfSnippet = new Put(hashFact.getHash());
			put.setWriteToWAL(false); // XXX performance increase
			hfSnippet.setWriteToWAL(false); // XXX performance increase
			try {
				hashFact.save(put);
				facts.put(put);
				hashFact.saveSnippet(hfSnippet);
				hashfactContent.put(hfSnippet);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

}
