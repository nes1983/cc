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
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Location;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.RealCodeFile;
import ch.unibe.scg.cc.activerecord.Snippet;
import ch.unibe.scg.cc.activerecord.Version;

import com.google.inject.Inject;

@Singleton
public class CloneRegistry {

	public static final int TABLE_PROJECT2VERSION = 1;
	public static final int TABLE_FILE2FUNCTION = 2;
	public static final int TABLE_FUNCTION2SNIPPET = 3;
	public static final int TABLE_VERSION2FILE = 4;
	public static final int TABLE_STRINGS = 5;

	final Provider<Snippet> snippetProvider;

	final Provider<Location> locationProvider;

	@Inject(optional = true)
	@Named("project2version")
	HTable project2version;

	@Inject(optional = true)
	@Named("version2file")
	HTable version2file;

	@Inject(optional = true)
	@Named("file2function")
	HTable file2function;

	@Inject(optional = true)
	@Named("function2snippet")
	HTable function2snippet;

	@Inject(optional = true)
	@Named("strings")
	HTable strings;

	@Inject
	public CloneRegistry(Provider<Snippet> hashFactProvider, Provider<Location> locationProvider) {
		this.snippetProvider = hashFactProvider;
		this.locationProvider = locationProvider;
	}

	public void register(byte[] hash, String snippetValue, Function function, Location location, byte type) {
		Snippet snippet = snippetProvider.get();
		snippet.setHash(hash);
		snippet.setSnippet(snippetValue);
		snippet.setLocation(location);
		snippet.setFunction(function);
		snippet.setType(type);
		function.addSnippet(snippet);
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
		HTable t = getTable(CloneRegistry.TABLE_FILE2FUNCTION);
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
		HTable t = getTable(CloneRegistry.TABLE_VERSION2FILE);
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
		HTable t = getTable(CloneRegistry.TABLE_PROJECT2VERSION);
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
		case TABLE_PROJECT2VERSION:
			return project2version;
		case TABLE_FILE2FUNCTION:
			return file2function;
		case TABLE_FUNCTION2SNIPPET:
			return function2snippet;
		case TABLE_VERSION2FILE:
			return version2file;
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
				// save function
				put.add(RealCodeFile.FAMILY_NAME, function.getHash(), 0l, Bytes.toBytes(function.getBaseLine()));
				file2function.put(put);

				// save snippet
				Put fnSnippet = new Put(function.getHash());
				function.saveSnippet(fnSnippet);
				strings.put(fnSnippet);
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
			project2version.put(put);
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
			version2file.put(put);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void register(Function function) {
		for (Snippet snippet : function.getSnippets()) {
			Put put = new Put(function.getHash());
			Put hfSnippet = new Put(snippet.getHash());
			put.setWriteToWAL(false); // XXX performance increase
			hfSnippet.setWriteToWAL(false); // XXX performance increase
			try {
				// save snippet
				snippet.save(put);
				function2snippet.put(put);

				// save snippetValue
				snippet.saveSnippet(hfSnippet);
				strings.put(hfSnippet);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

}
