package ch.unibe.scg.cc.activerecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;

import ch.unibe.scg.cc.StandardHasher;

public class Function extends Column {

	private List<HashFact> hashFacts;
	private int baseLine;
	transient StringBuilder contents;
	private byte[] hash;
	private boolean isOutdatedHash;

	@Inject
	StandardHasher standardHasher;
	
	public Function() {
		this.hashFacts = new ArrayList<HashFact>();
		this.isOutdatedHash = true;
	}

	public void save(Put put) throws IOException {
		// nothing to do
	}

	@Override
	public byte[] getHash() {
		assert getContents() != null;
		if(isOutdatedHash)
			this.hash = standardHasher.hash(getContents().toString());
		return this.hash;
	}

	public int getBaseLine() {
		return baseLine;
	}

	public void setBaseLine(int baseLine) {
		this.baseLine = baseLine;
	}

	public StringBuilder getContents() {
		return contents;
	}

	public void setContents(StringBuilder contents) {
		this.contents = contents;
		this.isOutdatedHash = true;
	}

	public void setContents(String contents) {
		setContents(new StringBuilder(contents));
	}
	
	public void addHashFact(HashFact hashFact) {
		this.hashFacts.add(hashFact);
	}
	
	public List<HashFact> getHashFacts() {
		return this.hashFacts;
	}

}
