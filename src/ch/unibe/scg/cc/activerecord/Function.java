package ch.unibe.scg.cc.activerecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;

import ch.unibe.scg.cc.CannotBeHashedException;
import ch.unibe.scg.cc.Hasher;
import ch.unibe.scg.cc.util.ByteUtils;

import com.google.inject.assistedinject.Assisted;

public class Function extends Column {
	final private List<HashFact> hashFacts;
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
		this.hashFacts = new ArrayList<HashFact>();
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

	public void addHashFact(HashFact hashFact) {
		this.hashFacts.add(hashFact);
	}

	public List<HashFact> getHashFacts() {
		return this.hashFacts;
	}

}
