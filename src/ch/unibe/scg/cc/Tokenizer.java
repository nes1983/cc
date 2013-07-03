package ch.unibe.scg.cc;

import ch.unibe.scg.cc.Protos.Function;

public interface Tokenizer {
	public Iterable<Function> tokenize(String file);
}