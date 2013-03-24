package ch.unibe.scg.cc;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Logger;

import javax.inject.Named;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.IPutFactory;
import ch.unibe.scg.cc.activerecord.Location;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.Snippet;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.mappers.HTableWriteBuffer;

import com.google.common.collect.Maps;
import com.google.inject.Inject;

/**
 * The DuplicateSnippetsPerFunctionRegistry stores information about duplicate
 * snippets in one function. We want to measure how often our assumption
 * "one snippet occurs at max. once per function" is violated.
 */
public class DuplicateSnippetsPerFunctionRegistry implements Registry, Closeable {
	final static Logger logger = Logger.getLogger(DuplicateSnippetsPerFunctionRegistry.class.getName());
	final private static byte[] TYPE_1_FAMILY = Bytes.toBytes("t1");
	final private static byte[] TYPE_2_FAMILY = Bytes.toBytes("t2");
	final private static byte[] TYPE_3_FAMILY = Bytes.toBytes("t3");
	final CloneRegistry cloneRegistry;
	final IPutFactory putFactory;

	@Inject(optional = true)
	@Named("duplicateSnippetsPerFunction")
	HTableWriteBuffer duplicateSnippetsPerFunction;

	@Inject
	public DuplicateSnippetsPerFunctionRegistry(CloneRegistry cloneRegistry, IPutFactory putFactory) {
		this.cloneRegistry = cloneRegistry;
		this.putFactory = putFactory;
	}

	public void register(byte[] hash, String snippetValue, Function function, Location location, byte type) {
		cloneRegistry.register(hash, snippetValue, function, location, type);
	}

	public void register(byte[] hash, String snippet, Function function, int from, int length, byte type) {
		cloneRegistry.register(hash, snippet, function, from, length, type);
	}

	public void register(CodeFile codeFile) {
	}

	public void register(Project project) {
	}

	public void register(Version version) {
	}

	public void register(Function function) {
		final HashMap<String, Integer> snippetHashes = Maps.newHashMap();
		for (final Snippet snippet : function.getSnippets()) {
			// Duplicate Hashes detection
			final String snippetHash = ByteUtils.bytesToHex(snippet.getHash());
			if (snippetHashes.containsKey(snippetHash)) {
				final Put put = putFactory.create(function.getHash());
				final byte typebyte = snippet.getType();
				byte[] family;
				switch (typebyte) {
				case Main.TYPE_1_CLONE:
					family = TYPE_1_FAMILY;
					break;
				case Main.TYPE_2_CLONE:
					family = TYPE_2_FAMILY;
					break;
				case Main.TYPE_3_CLONE:
					family = TYPE_3_FAMILY;
					break;
				default:
					throw new AssertionError("Unknow Clone-type: " + typebyte);
				}

				// don't store the type byte in the column name
				// it's already stored in the family name
				put.add(family, Bytes.tail(snippet.getHash(), 20), 0l, Bytes.toBytes(snippet.getSnippet()));
				try {
					duplicateSnippetsPerFunction.write(put);
				} catch (IOException e) {
					throw new RuntimeException("Snippet " + snippetHash + ": " + e);
				}
			} else {
				snippetHashes.put(snippetHash, 1);
			}
		}
	}

	@Override
	public void close() throws IOException {
		cloneRegistry.close();
	}
}
