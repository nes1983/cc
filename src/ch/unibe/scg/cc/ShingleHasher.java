package ch.unibe.scg.cc;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;

import javax.inject.Inject;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import dk.brics.automaton.AutomatonMatcher;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;

public class ShingleHasher implements Hasher {
	private final MessageDigest md;

	final private static int SHINGLE_LENGTH = 4;
	final private RunAutomaton shingleRegex =  new RunAutomaton(new RegExp("[^\\ ]+\\ [^\\ ]+\\ [^\\ ]+\\ [^\\ ]+").toAutomaton());

	final int SHA1_LENGTH = 20;

	@Inject
	ShingleHasher(MessageDigest md) {
		this.md = md;
	}

	Collection<String> shingles(String doc) throws CannotBeHashedException {
		Collection<String> ret = new ArrayList<>();
		int start = 0;
		for (int i = 0; i < SHINGLE_LENGTH; i++) {
			AutomatonMatcher matcher = shingleRegex.newMatcher(doc, start, doc.length());
			while (matcher.find()) {
				ret.add(matcher.group());
			}
			start = doc.indexOf(' ', start + 1);
			if (start == -1) {
				throw new CannotBeHashedException();
			}
		}

		return ret;
	}

	Iterable<ByteBuffer> hashedShingles(Collection<String> shingles) {
		// LinkedHashSet maintains order, but deletes duplicates.
		LinkedHashSet<ByteBuffer> hashed = Sets.newLinkedHashSetWithExpectedSize(shingles.size());
		for (String shingle : shingles) {
			hashed.add(ByteBuffer.wrap(md.digest(shingle.getBytes(Charsets.UTF_8))));
		}
		return hashed;
	}

	/**
	 * Use a quarter of all hashes.
	 */
	private byte[] sketchFromHashedShingles(Iterable<ByteBuffer> hashedShingles, String doc) {
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
				Utils.xor(hash, hashedShingle);
			}
			if (!isZero(hash)) {
				return hash;
			}
		} while (mask != 0); // In the last run, mask is zero. Zero must turn up a hash.
		throw new AssertionError("After mask was 0, there must be a hash. Input:\n" + doc);
	}

	@Override
	public byte[] hash(String doc) throws CannotBeHashedException {
		return sketchFromHashedShingles(hashedShingles(shingles(doc)), doc);
	}

	private boolean isZero(byte[] ary) {
		for (final byte element : ary) {
			if (element != 0) {
				return false;
			}
		}
		return true;
	}
}
