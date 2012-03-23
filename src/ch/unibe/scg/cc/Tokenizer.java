package ch.unibe.scg.cc;

import java.util.List;

import ch.unibe.scg.cc.activerecord.Function;

public interface Tokenizer {

	public abstract List<Function> tokenize(String file, String fileName);

}