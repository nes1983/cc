package ch.unibe.scg.cc.regex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


class ReplacementString implements Serializable {
	final private static long serialVersionUID = 1L;

	final List<Segment> contents = new ArrayList<>();
	private static final Pattern p = Pattern.compile("\\$(\\d+)");

	public ReplacementString(String with) {
		Matcher m = p.matcher(with);
		int lastFind = 0;
		while (m.find()) {

			addUnlessEmpty(contents, new LiteralSegment(with.substring(lastFind, m.start(1) - 1)));
			contents.add(new PlaceHolderSegment(Integer.parseInt(m.group(1))));

			lastFind = m.end();
		}
		addUnlessEmpty(contents, new LiteralSegment(with.substring(lastFind, with.length())));
	}

	// Called from constructor and therefore final.
	final void addUnlessEmpty(List<Segment> list, LiteralSegment segment) {
		if (segment.s.equals("")) {
			return;
		}
		list.add(segment);
	}

	public String fillIn(MatchResult matchResult) {
		StringBuilder sb = new StringBuilder();
		for (Segment s : contents) {
			sb.append(s.fillIn(matchResult));
		}
		return sb.toString();
	}

	static abstract class Segment implements Serializable {
		final private static long serialVersionUID = 1L;

		abstract String fillIn(MatchResult matchResult);
	}

	static class PlaceHolderSegment extends Segment {
		final private static long serialVersionUID = 1L;
		final int placeHolderNumber;

		public PlaceHolderSegment(int placeHolderNumber) {
			this.placeHolderNumber = placeHolderNumber;
		}

		@Override
		String fillIn(MatchResult m) {
			return m.group(placeHolderNumber);
		}
	}

	static class LiteralSegment extends Segment {
		final private static long serialVersionUID = 1L;

		final String s;

		public LiteralSegment(String s) {
			this.s = s;
		}

		@Override
		String fillIn(MatchResult m) {
			return s;
		}
	}
}
