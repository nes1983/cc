package ch.unibe.scg.cc.lines;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.TransformerUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import ch.unibe.scg.cc.javaFrontend.JavaTokenizer;

public class ModifiableLines {
	final StringBuilder string;
	final List<LineBreak> lineBreaks;
	final Comparator<LineBreak> comparator;

	ModifiableLines(StringBuilder string, List<LineBreak> lineBreaks) {
		this.string = string;
		this.lineBreaks = lineBreaks;

		comparator = new Comparator<LineBreak>() {
			@Override
			public int compare(LineBreak o1, LineBreak o2) {
				if (o1.position == o2.position) {
					return 0;
				}
				return o1.position > o2.position ? 1 : -1;
			}
		};
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
		return string.substring(lineBreaks.get(from).position, lineBreaks.get(from + length).position + 1);
	}

	/**
	 * Replaces the string from i to j, preserving line numbers.
	 * 
	 * @param from
	 * @param to
	 * @param replacement
	 *            May not contain character <code>'\n'</code>.
	 */
	public void replace(int from, int to, String replacement) {
		assert replacement.indexOf('\n') == -1;
		int start = Collections.binarySearch(lineBreaks, new LineBreak(from, 0), comparator);
		int end = Collections.binarySearch(lineBreaks, new LineBreak(to, 0), comparator);
		start = Math.abs(start);
		if (end < 0) {
			end = -end - 2;
		}
		removeFromTo(lineBreaks, start, end);

		int removedLines = positivePart(end - start + 1);
		lineBreaks.get(start).weight += removedLines;
		final int shift = replacement.length() - (to - from);
		for (int i = end; i < lineBreaks.size(); i++) {
			lineBreaks.get(i).position += shift;
		}

		string.replace(from, to, replacement);
	}

	int positivePart(int i) {
		if (i < 0)
			return 0;
		return i;
	}

	void removeFromTo(List<?> list, int start, int end) {
		assert end >= start;
		for (int i = start; i <= end; i++) {
			list.remove(i);
		}
	}

	public String toString() {
		return string.toString();
	}
}
