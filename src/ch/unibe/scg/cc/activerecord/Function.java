package ch.unibe.scg.cc.activerecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;

import ch.unibe.scg.cc.StandardHasher;

import com.google.inject.assistedinject.Assisted;

public class Function extends Column {

	final private List<HashFact> hashFacts;
	final private int baseLine;
	final transient CharSequence contents;
	private byte[] hash = null;

	final StandardHasher standardHasher;

	public static interface FunctionFactory {
		Function makeFunction(int baseLine, CharSequence contents);
	}

	@Inject
	/**
	 * Creates a new function by copying only "contents", "baseline" and
	 * "standardHasher" from the provided function.
	 */
	public Function(StandardHasher standardHasher, @Assisted int baseLine,
			@Assisted CharSequence contents) {
		this.hashFacts = new ArrayList<HashFact>();
		this.standardHasher = standardHasher;
		this.baseLine = baseLine;
		this.contents = contents;
	}

	public void save(Put put) throws IOException {
		// nothing to do
	}

	public byte[] getHash() {
		assert getContents() != null;
		if (hash == null) {
			hash = standardHasher.hash(getContents().toString());
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
