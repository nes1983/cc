package ch.unibe.scg.cc.lines;

import java.io.Serializable;

/** Factory for {@link StringOfLines}. */
public class StringOfLinesFactory implements Serializable {
	private static final long serialVersionUID = 1L;

	StringOfLinesFactory() {} // Prevent subclassing.

	/** @return a new instance. */
	public static StringOfLinesFactory getInstance() {
		return new StringOfLinesFactory();
	}

	/** @return string, split by separator. */
	public StringOfLines make(String string, char separator) {
		String stringEndingInNewline = string;
		if (!string.endsWith(String.valueOf(separator))) {
			stringEndingInNewline = string + separator;
		}
		return makeSanitized(stringEndingInNewline, separator);
	}

	private StringOfLines makeSanitized(String string, char separator) {
		int count = countOccurrences(string, separator);
		int[] separators = new int[count + 1];
		separators[0] = 0;
		int thisIndex = string.indexOf(separator);
		int i;
		for (i = 1; thisIndex != -1; thisIndex = string.indexOf(separator, thisIndex + 1)) {
			separators[i] = thisIndex;
			i++;
		}
		assert i == separators.length : "" + (separators.length) + " : " + i;
		return new StringOfLines(string, separators);

	}

	/** @return the number of occurrences of needle in haystack. */
	public static int countOccurrences(String haystack, char needle) { // TODO: should not be public …
		int count = 0;
		for (int i = 0; i < haystack.length(); i++) {
			if (haystack.charAt(i) == needle) {
				count++;
			}
		}
		return count;
	}
}