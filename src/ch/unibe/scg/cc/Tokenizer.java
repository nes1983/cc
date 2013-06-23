package ch.unibe.scg.cc;


public interface Tokenizer {
	public Iterable<SnippetWithBaseline> tokenize(String file);
}