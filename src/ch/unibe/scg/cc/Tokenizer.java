package ch.unibe.scg.cc;

import ch.unibe.scg.cc.Protos.Function;

/** A Tokenizer tokenizes a source file into Functions. */
public interface Tokenizer {
	/** Split the file into functions. It is tolerable to make mistakes. */
	Iterable<Function> tokenize(String file);
}