package ch.unibe.scg.cc;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Named;
import javax.inject.Provider;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.IPutFactory;
import ch.unibe.scg.cc.activerecord.Location;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.RealCodeFile;
import ch.unibe.scg.cc.activerecord.Snippet;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.mappers.HTableWriteBuffer;

import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;

public class CloneRegistry implements Registry, Closeable {
	final static Logger logger = Logger.getLogger(CloneRegistry.class.getName());
	final Provider<Snippet> snippetProvider;
	final Provider<Location> locationProvider;
	final IPutFactory putFactory;

	/** We cache whether or not files and functions have been written. */
	private static final int CACHE_SIZE = 1000000;
	/** Functions that were successfully written to DB in this mapper */
	final Map<ByteBuffer, Boolean> writtenFunctions =
			CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).<ByteBuffer, Boolean>build().asMap();
	/** Files that were successfully written to DB in this mapper */
	final Map<ByteBuffer, Boolean> writtenFiles =
			CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).<ByteBuffer, Boolean>build().asMap();

	@Inject(optional = true)
	@Named("project2version")
	HTableWriteBuffer project2version;

	@Inject(optional = true)
	@Named("version2file")
	HTableWriteBuffer version2file;

	@Inject(optional = true)
	@Named("file2function")
	HTableWriteBuffer file2function;

	@Inject(optional = true)
	@Named("function2snippet")
	HTableWriteBuffer function2snippet;

	@Inject(optional = true)
	@Named("strings")
	HTableWriteBuffer strings;

	@Inject
	public CloneRegistry(Provider<Snippet> hashFactProvider, Provider<Location> locationProvider, IPutFactory putFactory) {
		this.snippetProvider = hashFactProvider;
		this.locationProvider = locationProvider;
		this.putFactory = putFactory;
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

	public void register(CodeFile codeFile) {
		// TODO write codeFile string into strings table.
		if (writtenFiles.containsKey(ByteBuffer.wrap(codeFile.getHash()))) {
			return; // We've dealt with this file.
		}
		writtenFiles.put(ByteBuffer.wrap(codeFile.getHash()), true);

		for (Function function : codeFile.getFunctions()) {
			Put functionPut = putFactory.create(codeFile.getHash());
			try {
				functionPut.add(RealCodeFile.FAMILY_NAME, function.getHash(), 0l, Bytes.toBytes(function.getBaseLine()));
				file2function.write(functionPut);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	/** Saves the project in the projects table. */
	public void register(Project project) {
		Put put = putFactory.create(project.getHash());
		try {
			project.save(put);
			project2version.write(put);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/** Saves the version in the versions table. */
	public void register(Version version) {
		Put put = putFactory.create(version.getHash());
		try {
			version.save(put);
			version2file.write(put);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void register(Function function) {
		if (writtenFunctions.containsKey(ByteBuffer.wrap(function.getHash()))) {
			return; // This function has been dealt with.
		}
		writtenFunctions.put(ByteBuffer.wrap(function.getHash()), true);

		Put functionString = putFactory.create(function.getHash());
		try {
			function.saveContents(functionString);
			strings.write(functionString);

			for (Snippet snippet : function.getSnippets()) {
				Put put = putFactory.create(function.getHash());
				snippet.save(put);
				function2snippet.write(put);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void close() throws IOException {
		project2version.close();
		version2file.close();
		file2function.close();
		function2snippet.close();
		strings.close();
	}
}
