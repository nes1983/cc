package ch.unibe.scg.cc.lines;

import org.apache.commons.lang3.ArrayUtils;

public class StringOfLines {
	final String string;
	final int[] separators;

	StringOfLines(String string, int[] separators) {
		this.string = string;
		this.separators = separators;
	}

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

}