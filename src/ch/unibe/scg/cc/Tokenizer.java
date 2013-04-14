package ch.unibe.scg.cc;

import com.google.common.collect.ComparisonChain;

public interface Tokenizer {
	public static class SnippetWithBaseline implements Comparable<SnippetWithBaseline> {
		final int baseLine;
		final CharSequence snippet;

		public SnippetWithBaseline(int baseLine, CharSequence snippet) {
			this.baseLine = baseLine;
			this.snippet = snippet;
		}

		public int getBaseLine() {
			return baseLine;
		}

		public CharSequence getSnippet() {
			return snippet;
		}

		// For testing only.
		@Override
		public int compareTo(SnippetWithBaseline o) {
			return ComparisonChain.start()
					.compare(baseLine, o.baseLine)
					.compare(snippet.toString(), o.snippet.toString())
					.result();
		}
	}

	public Iterable<SnippetWithBaseline> tokenize(String file);
}