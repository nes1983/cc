package ch.unibe.scg.cc.javaFrontend;

import java.util.regex.Pattern;

import ch.unibe.scg.cc.ReplacerProvider;
import ch.unibe.scg.cc.regex.Replace;

// TODO: Should not be public. It's only public because a silly test depends on it.
public class JavaType1ReplacerFactory extends ReplacerProvider {
	final private static long serialVersionUID = 1L;

	/**
	 * 0
	 */
	public Replace make00WhitespaceA() {
		Pattern whiteSpace = Pattern.compile("\\s*\\n\\s*");
		return new Replace(whiteSpace, "\n");
	}

	/**
	 * 1
	 */
	public Replace make01WhitespaceB() {
		Pattern whiteSpace = Pattern.compile("[ \f\r\t]+");
		return new Replace(whiteSpace, " ");
	}

	/**
	 * 2. removes package prefixes. <br>
	 * <code>example 1:<br>"import java.util.Math;" gets "import Math;"</code><br>
	 * <code>example 2:<br>
	 * "int a = java.util.Math.sqrt(1);" gets "int a = Math.sqrt(1);"</code>
	 */
	public Replace make02Rj1() {
		final Pattern packagesGo = Pattern.compile("\\b[a-z]+[a-z.]*\\.([A-Z])");
		final String replaceWith = "$1";
		return new Replace(packagesGo, replaceWith);
	}

	/**
	 * 3
	 */
	public Replace make03Rj3a() {
		Pattern initializationList = Pattern.compile("=\\s?\\{.*?\\}");
		return new Replace(initializationList, "= { }");
	}

	/**
	 * 4
	 */
	public Replace make04Rj3b() {
		Pattern initializationList = Pattern.compile("\\]\\s?\\{.*?\\}");
		return new Replace(initializationList, "] { }");
	}

	/**
	 * 5
	 */
	public Replace make05Rj5() {
		Pattern visibility = Pattern.compile("(\\s)(?:private\\s|public\\s|protected\\s)");
		return new Replace(visibility, "$1");
	}

	/**
	 * 6
	 */
	public Replace make06Rj6() {
		Pattern ifPattern = Pattern.compile("\\sif\\s*\\((?:.*)\\)\\s*(\\n[^\\n\\{\\}]*)$");
		return new Replace(ifPattern, " {\\n$1\\n}\\n");
	}

	/** 7. This is necessary for tokenization. while statements look a lot like function definitions. */
	public Replace make07RenameWhile() {
		Pattern ifPattern = Pattern.compile("while");
		return new Replace(ifPattern, ";while");
	}

	/**
	 * 8. removes leading whitespace at the beginning of the string. example:
	 * "	 public void a() {" gets "public void a() {"
	 */
	public Replace make08RemoveLeadingWhitespace() {
		Pattern leadingWhiteSpace = Pattern.compile("^[ \f\r\t]*");
		return new Replace(leadingWhiteSpace, "");
	}
}
