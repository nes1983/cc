package ch.unibe.scg.cc.regex;

import java.util.ArrayList;
import java.util.List;

import jregex.MatchResult;
import jregex.Matcher;
import jregex.Pattern;

class ReplacementString {
	final ArrayList<Segment> contents = new ArrayList<Segment>();
	static final Pattern p = new Pattern("\\$(\\d+)");
	public ReplacementString(String with) {
		 
		Matcher m = p.matcher(with);
		int lastFind = 0;
		while(m.find()) {
			
			addUnlessEmpty(contents, new LiteralSegment(with.substring(lastFind, m.start(1)-1)));
			contents.add(new PlaceHolderSegment(Integer.parseInt(m.group(1))));
			
			lastFind = m.end();
		}
		addUnlessEmpty(contents, new LiteralSegment(with.substring(lastFind, with.length())));
	}
	
	void addUnlessEmpty(List<Segment> list, LiteralSegment segment) {
		if(segment.s.equals("")) 
			return;
		list.add(segment);
	}
	
	
	
	public String fillIn(MatchResult matchResult) {
		StringBuilder sb = new StringBuilder();
		for(Segment s : contents) {
			sb.append(s.fillIn(matchResult));
		}
		return sb.toString();
	}
	
	static abstract class Segment {
		abstract String fillIn(MatchResult matchResult);
	}
	
	static class PlaceHolderSegment extends Segment {
		int placeHolderNumber;
		public PlaceHolderSegment(int placeHolderNumber) {
			this.placeHolderNumber = placeHolderNumber;
		}
		
		String fillIn(MatchResult m) {
			return m.group(placeHolderNumber);
		}
	}
	
	static class LiteralSegment extends Segment {
		String s;
		public LiteralSegment(String s) {
			this.s = s;
		}
		@Override
		String fillIn(MatchResult m) {
			return s;
		}
	}
}