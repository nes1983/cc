package ch.unibe.scg.cc.lines;

/** A string with efficient access into individual lines. First line has position 0. */
public class StringOfLines {
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
		return string.substring(separators[from], separators[from + length] + 1);
	}

	@Override
	public String toString() {
		return string;
	}
}