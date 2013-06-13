package ch.unibe.scg.cc.mappers;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import javax.inject.Inject;


import com.google.common.base.Preconditions;

public class HashSerializer {
	private final Comparator<byte[]> byteArrayComparator;

	@Inject
	HashSerializer(Comparator<byte[]> byteArrayComparator) {
		this.byteArrayComparator = byteArrayComparator;
	}

	/** @param set of byte[], all sharing the same size. */
	public byte[] serialize(Set<byte[]> set) {
		if (set.size() == 0) {
			return new byte[] {};
		}

		checkProperSizes(set);

		int size = getSize(set);
		byte[] result = new byte[size * set.size()];

		int tgtOffset = 0;
		for (byte[] b : set) {
			System.arraycopy(b, 0, result, tgtOffset, size);
			// Bytes.writeByteArray(result, tgtOffset, b, 0, size);
			tgtOffset += size;
		}
		return result;
	}

	int getSize(Set<byte[]> set) {
		return set.iterator().next().length;
	}

	void checkProperSizes(Set<byte[]> set) {
		int size = set.iterator().next().length;
		for (byte[] b : set) {
			assert b.length == size;
		}
	}

	/** {@code array.length % sizeOfSingleEntry == 0} should hold */
	public Set<byte[]> deserialize(byte[] array, int sizeOfSingleEntry) {
		Preconditions.checkArgument(array.length % sizeOfSingleEntry == 0);

		Set<byte[]> result = new TreeSet<byte[]>(byteArrayComparator);

		for (int i = 0; i * sizeOfSingleEntry < array.length; i++) {
			byte[] target = new byte[sizeOfSingleEntry];
			int offset = i * sizeOfSingleEntry;
			System.arraycopy(array, offset, target, 0, sizeOfSingleEntry);
			result.add(target);
		}
		return Collections.unmodifiableSet(result);
	}
}
