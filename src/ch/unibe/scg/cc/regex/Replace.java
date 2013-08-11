package ch.unibe.scg.cc.regex;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/** Replace is a command (as in command pattern) to replace a regex in a string for a substitution string. */
public final class Replace implements Serializable {
	final private static long serialVersionUID = 1L;

	private final Pattern pattern;
	final ReplacementString replacementString;

	/** New Replace that replaces all matches of {@code pattern} with replacement {@code with}. */
	public Replace(Pattern pattern, String with) {
		this.pattern = pattern;
		replacementString = new ReplacementString(with);
	}

	/** Replace all occurrences in {@code sb}, in place. */
	public void replaceAll(StringBuilder sb) {
		int offset = 0;
		Matcher matcher = pattern.matcher(sb.toString());
		while (matcher.find()) {
			String replacement = replacementString.fillIn(matcher);
			int start = matcher.start();
			int end = matcher.end();
			sb.replace(start + offset, end + offset, replacement);
			int matchLength = matcher.end() - matcher.start();
			offset += replacement.length() - matchLength;
		}
	}

	// TODO: Too trivial to export. Should not be public.
	public String allReplaced(CharSequence cs) {
		StringBuilder sb = new StringBuilder(cs);
		replaceAll(sb);
		return sb.toString();
	}
}
