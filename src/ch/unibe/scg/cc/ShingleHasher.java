package ch.unibe.scg.cc;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashSet;

import javax.inject.Inject;

import com.google.common.base.Preconditions;

import dk.brics.automaton.AutomatonMatcher;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;

public class ShingleHasher implements Hasher {

	@Inject
	MessageDigest md;

	final int SHINGLE_LENGTH = 4;
	RunAutomaton shingleRegex;

	final int SHA1_LENGTH = 20;

	public ShingleHasher() {
		RegExp regex = new RegExp("[^\\ ]+\\ [^\\ ]+\\ [^\\ ]+\\ [^\\ ]+");
		shingleRegex = new RunAutomaton(regex.toAutomaton());
	}

	final String[] stringArrayType = new String[] {};

	String[] shingles(String doc) throws CannotBeHashedException {
		ArrayList<String> shingles = new ArrayList<String>();
		int start = 0;
		for (int i = 0; i < SHINGLE_LENGTH; i++) {
			AutomatonMatcher matcher = shingleRegex.newMatcher(doc, start, doc.length());
			while (matcher.find()) {
				shingles.add(matcher.group());
			}
			start = doc.indexOf(' ', start + 1);
			if (start == -1) {
				throw new CannotBeHashedException();
			}
		}

		return shingles.toArray(stringArrayType);
	}

	Iterable<ByteBuffer> hashedShingles(String[] shingles) {
		// LinkedHashSet maintains order, but deletes duplicates.
		LinkedHashSet<ByteBuffer> hashed = new LinkedHashSet<ByteBuffer>(shingles.length);
		for (String shingle : shingles) {
			hashed.add(ByteBuffer.wrap(md.digest(shingle.getBytes())));
		}
		return hashed;
	}

	/**
	 * Use a quarter of all hashes.
	 */
	byte[] sketchFromHashedShingles(Iterable<ByteBuffer> hashedShingles, String doc) {
		Preconditions.checkArgument(hashedShingles.iterator().hasNext(),
				"There was nothing to make a sketch from. Input:\n" + doc);
		final byte[] hash = new byte[SHA1_LENGTH];
		int mask = 0x7; // After the first shift, that'll give binary pattern 11.
		do {
			mask >>= 1;
			for (final ByteBuffer hashedShingleBuffer : hashedShingles) {
				final byte[] hashedShingle = hashedShingleBuffer.array();
				if ((hashedShingle[0] & mask) != mask) {
					continue;
				}
				xor(hash, hashedShingle);
			}
			if (!isZero(hash)) {
				return hash;
			}
		} while (mask != 0); // In the last run, mask is zero. Zero must turn up a hash.
		throw new AssertionError("After mask was 0, there must be a hash. Input:\n" + doc);
	}

	public byte[] hash(String doc) throws CannotBeHashedException {
		return sketchFromHashedShingles(hashedShingles(shingles(doc)), doc);
	}

	/**
	 * Performs hash = hash XOR otherHash. Changes hash in place.
	 */
	// TODO: compare performance with BitSet.
	void xor(byte[] hash, byte[] otherHash) {
		assert hash.length == SHA1_LENGTH;
		for (int i = 0; i < hash.length; i++) {
			hash[i] = (byte) (hash[i] ^ otherHash[i]);
		}
	}

	boolean isZero(byte[] ary) {
		for (final byte element : ary) {
			if (element != 0) {
				return false;
			}
		}
		return true;
	}
}
