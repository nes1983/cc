package ch.unibe.scg.cc;

import java.io.Serializable;

import ch.unibe.scg.cc.Protos.Function;

/** A Tokenizer tokenizes a source file into Functions. */
public interface Tokenizer extends Serializable {
	/** Split the file into functions. It is tolerable to make mistakes. */
	Iterable<Function> tokenize(String file);
}