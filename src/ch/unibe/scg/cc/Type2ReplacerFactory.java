package ch.unibe.scg.cc;

import java.util.regex.Pattern;

import ch.unibe.scg.cc.regex.Replace;

class Type2ReplacerFactory extends ReplacerProvider {
	final private static long serialVersionUID = 1L;

	public Replace make00WordsIntoTs() {
		Pattern word = Pattern.compile("[a-zA-Z]+");
		return new Replace(word, "t");
	}

	public Replace make01NumbersIntoOnes() {
		Pattern number = Pattern.compile("\\d+");
		return new Replace(number, "1");
	}

	public Replace make02Tokenize() {
		Pattern invoke = Pattern.compile("(\\w)\\.(\\w)");
		return new Replace(invoke, "$1. $2");
	}

	/**
	 * make02Tokenize() does not find all matches. example: "t.t.t" gets
	 * "t. t.t" therefore we call make02Tokenize() a second time
	 */
	public Replace make03Tokenize() {
		return make02Tokenize();
	}
}
