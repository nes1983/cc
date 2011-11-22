package ch.unibe.scg.cc.regex;

import ch.unibe.scg.cc.lines.ModifiableLines;
import jregex.Matcher;
import jregex.Pattern;

/**
 * Replace is a command (as in command pattern) to high-speed replaces a regex in a string for substitution string. 
 * @author nes
 *
 */
public class Replace {
	public final Pattern  pattern;
	final ReplacementString replacementString;

	public Replace(Pattern pattern, String with) {
		this.pattern = pattern;
		replacementString = new ReplacementString(with);
	}

	public void replaceAll(StringBuilder sb) {
		int offset = 0;
		Matcher matcher = pattern.matcher(sb.toString());
		while(matcher.find()) {
			String replacement = replacementString.fillIn(matcher);
			int start = matcher.start();
			int end = matcher.end();
			sb.replace(start+offset, end+offset, replacement);	
			int matchLength = matcher.end() - matcher.start();
			offset += replacement.length() - matchLength;
		} 
	}
	
	public void replaceAll(ModifiableLines lines) {
		int offset = 0;
		Matcher matcher = pattern.matcher(lines.toString());
		while(matcher.find()) {
			String replacement = replacementString.fillIn(matcher);
			int start = matcher.start();
			int end = matcher.end();
			lines.replace(start+offset, end+offset, replacement);	
			int matchLength = matcher.end() - matcher.start();
			offset += replacement.length() - matchLength;
		}
	}
	
	public String allReplaced(CharSequence cs) {
		StringBuilder sb = new StringBuilder(cs);
		replaceAll(sb);
		return sb.toString();
	}

}


