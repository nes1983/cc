package ch.unibe.scg.cc.activerecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.ByteUtils;
import ch.unibe.scg.cc.CannotBeHashedException;
import ch.unibe.scg.cc.Hasher;

import com.google.inject.assistedinject.Assisted;

public class Function extends Column {
	private static final byte[] FUNCTION_SNIPPET = Bytes.toBytes("fs");
	final private List<Snippet> snippets;
	final private int baseLine;
	final transient CharSequence contents;
	private byte[] hash = null;
	final Hasher hasher;

	public static interface FunctionFactory {
		Function makeFunction(Hasher hasher, int baseLine, CharSequence contents);
	}

	@Inject
	/**
	 * Creates a new function by copying only "contents", "baseline" and
	 * "standardHasher" from the provided function.
	 */
	public Function(@Assisted Hasher hasher, @Assisted int baseLine, @Assisted CharSequence contents) {
		this.snippets = new ArrayList<Snippet>();
		this.hasher = hasher;
		this.baseLine = baseLine;
		this.contents = contents;
	}

	public void save(Put put) throws IOException {
		// nothing to do
	}

	/**
	 * @return if the hasher throws a CannotBeHashedException the
	 *         {@link ByteUtils#EMPTY_SHA1_KEY} is returned.
	 */
	public byte[] getHash() {
		assert getContents() != null;
		if (hash == null) {
			try {
				hash = hasher.hash(getContents().toString());
			} catch (CannotBeHashedException e) {
				return ByteUtils.EMPTY_SHA1_KEY;
			}
		}
		return hash;
	}

	public int getBaseLine() {
		return baseLine;
	}

	public String getContents() {
		return contents.toString();
	}

	public void addSnippet(Snippet snippet) {
		this.snippets.add(snippet);
	}

	public List<Snippet> getSnippets() {
		return this.snippets;
	}

	public void saveSnippet(Put put) {
		put.add(FAMILY_NAME, FUNCTION_SNIPPET, 0l, Bytes.toBytes(getContents()));
	}
}
