package ch.unibe.scg.cc;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.ArrayList;

import javax.inject.Inject;

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

		return shingles.toArray((String[]) stringArrayType);
	}

	byte[][] hashedShingles(String[] shingles) {
		byte[][] hashed = new byte[shingles.length][];
		for (int i = 0; i < shingles.length; i++) {
			try {
				hashed[i] = md.digest(shingles[i].getBytes("UTF-16LE"));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}
		return hashed;
	}

	/**
	 * Use a quarter of all hashes.
	 */
	byte[] sketchFromHashedShingles(byte[][] hashedShingles) {
		final byte[] hash = new byte[SHA1_LENGTH];
		int mask = 0x3;
		do {
			for (final byte[] each : hashedShingles) {
				if ((each[0] & mask) != mask) {
					continue;
				}
				xor(hash, each);
			}
			if (!isZero(hash)) {
				return hash;
			}
			mask >>= 1;
		} while (mask != 0);
		throw new AssertionError("After mask was 0, there must be a hash.");
	}

	public byte[] hash(String doc) throws CannotBeHashedException {
		final String[] shingles = shingles(doc);
		final byte[][] hashedShingles = hashedShingles(shingles);
		return sketchFromHashedShingles(hashedShingles);
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
