package ch.unibe.scg.cells;

import java.util.Comparator;

import com.google.protobuf.ByteString;

/** Comparator for a string of unsigned bytes. Results in lexicographical ordering. */
public final class LexicographicalComparator implements Comparator<ByteString> {
	@Override
	public int compare(ByteString o1, ByteString o2) {
		for (int i = 0; i < Math.min(o1.size(), o2.size()); i++) {
			int v1 = o1.byteAt(i) & 0xff; // Remove sign.
			int v2 = o2.byteAt(i) & 0xff; // Remove sign.
			if (v1 == v2) {
				continue;
			}
			if (v1 < v2) {
				return -1;
			}
			return +1;
		}
		return o1.size() - o2.size();
	}
}