package ch.unibe.scg.cc.util;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import javax.inject.Inject;

import org.apache.hadoop.hbase.util.Bytes;

public class HashSerializer {
	
	private final Comparator<byte[]> byteArrayComparator;

	@Inject
	HashSerializer(Comparator<byte[]> byteArrayComparator) {
		this.byteArrayComparator = byteArrayComparator;
	}
	
	/**
	 * 
	 * @param set of byte[], all sharing the same size.
	 * @return
	 */
	public byte[] serialize(Set<byte[]> set) {
		if(set.size() == 0)  {
			return new byte[]{};
		}
		
		checkProperSizes(set);
		
		int size = getSize(set);
		byte[] result = new byte[size*set.size()];
		
		int tgtOffset = 0;
		for(byte[] b : set) {
			Bytes.writeByteArray(result, tgtOffset, b, 0, size);
			tgtOffset += size;
		}
		return result;
	}
	
	int getSize(Set<byte[]> set) {
		return set.iterator().next().length;
	}
	
	void checkProperSizes(Set<byte[]> set) {
		int size = set.iterator().next().length;
		for(byte[] b : set) {
			assert b.length == size;
		}
	}
	
	/**
	 * assert array.length % sizeOfSingleEntry == 0;
	 * @param array
	 * @param sizeOfSingleEntry
	 * @return
	 */
	public Set<byte[]> deserialize(byte[] array, int sizeOfSingleEntry) {
		Set<byte[]> result = new TreeSet<byte[]>(byteArrayComparator);
		
		assert array.length % sizeOfSingleEntry == 0;
		
		for(int i=0; i*sizeOfSingleEntry < array.length; i++) {
			byte[] target = new byte[sizeOfSingleEntry];
			Bytes.writeByteArray(target, 0, array, i*sizeOfSingleEntry, sizeOfSingleEntry);
			result.add(target);
		}
		return Collections.unmodifiableSet(result);
	}
}
