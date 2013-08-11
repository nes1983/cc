package ch.unibe.scg.cc.lines;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;

/** A string with efficient access into individual lines. First line has position 0. */
public class StringOfLines implements Serializable {
	final private static long serialVersionUID = 1L;

	final String string;
	final int[] separators;

	StringOfLines(String string, int[] separators) {
		this.string = string;
		this.separators = separators;
	}

	/** @return number of lines in the string. Empty string has length 1. */
	public int getNumberOfLines() {
		return separators.length - 1;
	}

	/**
	 * Get <code> length </code> lines, starting with <code>from</code>.
	 *
	 * @param length
	 *            Number of lines to return.
	 * @param from
	 *            Index of first returned line. Counting starts with 0.
	 */
	public String getLines(int from, int length) {
		checkArgument(from >= 0, "From was negative! " + from);
		checkArgument(length >= 0, "length was negative! " + length);
		checkArgument(from + length < separators.length,
				String.format("You're trying to access line %s (length %s), " +
						"but there are only %s lines", from+length, length, getNumberOfLines()));
		return string.substring(separators[from], separators[from + length] + 1);
	}

	@Override
	public String toString() {
		return string;
	}
}