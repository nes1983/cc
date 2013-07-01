package ch.unibe.scg.cc.javaFrontend;

import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import ch.unibe.scg.cc.SnippetWithBaseline;
import dk.brics.automaton.Automaton;
import dk.brics.automaton.AutomatonMatcher;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;

@SuppressWarnings("javadoc")
public class JavaTokenizerTest {
	@Test
	public void theRegexWorks() {
		String regex = JavaTokenizer.splitterRegex;
		Automaton a = new RegExp(regex).toAutomaton();
		RunAutomaton r = new RunAutomaton(a);

		assertFalse(r.newMatcher("String myString = new String(new int[] { 1, 2, 3});").find());
		assertFalse(r.newMatcher("return Integer.class").find());
		assertTrue(r.newMatcher("\nclass MyClass<String> {\n").find());
		assertTrue(r.newMatcher("\n     class MyClass<String> {").find());
	}

	@Test
	public void scumBagRegex() {
		String regex = "[\n]+";
		Automaton a = new RegExp(regex).toAutomaton();
		RunAutomaton r = new RunAutomaton(a);
		AutomatonMatcher m = r.newMatcher("\n\n\n");
		assertTrue(m.find());

		assertFalse(r.newMatcher("\\n").find());
	}

	@Test
	public void testSampleClass() {
		Iterator<SnippetWithBaseline> iter = new JavaTokenizer().tokenize(sampleClass()).iterator();

		assertThat(iter.next(), comparesEqualTo(new SnippetWithBaseline(4, "public class Rod {\n")));
		assertThat(iter.next(), comparesEqualTo(new SnippetWithBaseline(5,
						"\tpublic static void main(String[] args) {\n\t\tout.println(\"Hiya, wassup?\");\n\t\tfish.stink.Rod.doIt(new int[] { 1, 2 ,3 });\n\t}\n")));
		assertThat(iter.next(), comparesEqualTo(new SnippetWithBaseline(9,
						"\t\t\tprotected static void doIt() {\n\t\tif(System.timeInMillis() > 10000)\n\t\t\tout.println(1337);\n\t\tmain(null);\n\t}\n}\n")));
		assertThat(iter.hasNext(), is(false));
	}

	String sampleClass() {
		return "package        fish.stink;\n" + "import java.util.*;\n" + "import static System.out;\n\n"
				+ "public class Rod {\n" + "\tpublic static void main(String[] args) {\n"
				+ "\t\tout.println(\"Hiya, wassup?\");\n" + "\t\tfish.stink.Rod.doIt(new int[] { 1, 2 ,3 });\n"
				+ "	}\n" + "\t\t\tprotected static void doIt() {\n" + "\t\tif(System.timeInMillis() > 10000)\n"
				+ "\t\t\tout.println(1337);\n" + "\t\tmain(null);\n" + "\t}\n" + "}\n";
	}

	@Test
	public void testBiggerSampleClass() {
		Iterator<SnippetWithBaseline> iter = new JavaTokenizer().tokenize(biggerSampleClass()).iterator();
		assertThat(iter.next(), comparesEqualTo(new SnippetWithBaseline(4,
				"public class Math {\n"
						+ "\tstatic final int[] POWERS_OF_10 = { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };\n"
						+ "\tstatic final int[] HALF_POWERS_OF_10 = { 3, 31, 316, 3162, 31622, 316227, 3162277, 31622776, 316227766,\n"
						+ "\t\t\tInteger.MAX_VALUE };\n"
						+ "\tstatic final byte[] MAX_LOG_10_FOR_LEADING_ZEROS = { 9, 9, 9, 8, 8, 8, 7, 7, 7, 6, 6, 6, 6, 5, 5, 5, 4, 4, 4, 3, 3, 3, 3, 2, 2, 2, 1, 1, 1, 0, 0, 0, 0 };\n"
						+ "\n")));
		assertThat(iter.next(), comparesEqualTo(new SnippetWithBaseline(10,
				"\tpublic static int log10(int x, RoundingMode mode) {\n" + "\t\tcheckPositive(\"x\", x);\n"
						+ "\t\tint logFloor = log10Floor(x);\n" + "\t\tint floorPow = POWERS_OF_10[logFloor];\n"
						+ "\t\tint result = -1;\n" + "\t\tswitch (mode) {\n" + "\t\tcase UNNECESSARY:\n"
						+ "\t\t\tcheckRoundingUnnecessary(x == floorPow);\n" + "\t\t\t// fall through\n"
						+ "\t\tcase DOWN:\n" + "\t\t\tresult = logFloor;\n" + "\t\tcase CEILING:\n" + "\t\tcase UP:\n"
						+ "\t\t\tresult = (x == floorPow) ? logFloor : logFloor - 1;\n" + "\t\tcase HALF_DOWN:\n"
						+ "\t\tcase HALF_UP:\n" + "\t\tcase HALF_EVEN:\n"
						+ "\t\t\t// sqrt(10) is irrational, so log10(x) - logFloor is never exactly\n"
						+ "\t\t\t// 0.5\n"
						+ "\t\t\tresult = (x <= HALF_POWERS_OF_10[logFloor]) ? logFloor : logFloor - 1;\n" + "\t\t}\n"
						+ "\t\treturn result;\n" + "\t}\n" + "\n")));
		assertThat(iter.next(), comparesEqualTo(new SnippetWithBaseline(34,
				"\tprivate static int log10Floor(int x) {\n"
						+ "\t\tint y = MAX_LOG_10_FOR_LEADING_ZEROS[Integer.numberOfLeadingZeros(x)];\n"
						+ "\t\tint sgn = (x - POWERS_OF_10[y]) >>> (Integer.SIZE - 1);\n" + "\t\treturn y - sgn;\n"
						+ "\t}\n" + "\n")));
		assertThat(iter.next(), comparesEqualTo(new SnippetWithBaseline(40,
				"\tstatic void checkRoundingUnnecessary(boolean condition) {\n"
						+ "\t\tif (!condition) {\n"
						+ "\t\t\tthrow new ArithmeticException(\"mode was UNNECESSARY, but rounding was necessary\");\n"
						+ "\t\t}\n" + "\t}\n" + "\n")));
		assertThat(iter.next(), comparesEqualTo(new SnippetWithBaseline(46,
				"\tstatic int checkPositive(String role, int x) {\n" + "\t\tif (x <= 0) {\n"
						+ "\t\t\tthrow new IllegalArgumentException(role + \" (\" + x + \") must be > 0\");\n"
						+ "\t\t}\n" + "\t\treturn x;\n" + "\t}\n" + "}\n")));
		assertThat(iter.hasNext(), is(false));
	}

	String biggerSampleClass() {
		return "package com.stradivari.util;\n"
				+ "\n"
				+ "import java.math.RoundingMode;\n"
				+ "\n"
				+ "public class Math {\n"
				+ "\tstatic final int[] POWERS_OF_10 = { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };\n"
				+ "\tstatic final int[] HALF_POWERS_OF_10 = { 3, 31, 316, 3162, 31622, 316227, 3162277, 31622776, 316227766,\n"
				+ "\t\t\tInteger.MAX_VALUE };\n"
				+ "\tstatic final byte[] MAX_LOG_10_FOR_LEADING_ZEROS = { 9, 9, 9, 8, 8, 8, 7, 7, 7, 6, 6, 6, 6, 5, 5, 5, 4, 4, 4, 3, 3, 3, 3, 2, 2, 2, 1, 1, 1, 0, 0, 0, 0 };\n"
				+ "\n" + "\tpublic static int log10(int x, RoundingMode mode) {\n" + "\t\tcheckPositive(\"x\", x);\n"
				+ "\t\tint logFloor = log10Floor(x);\n" + "\t\tint floorPow = POWERS_OF_10[logFloor];\n"
				+ "\t\tint result = -1;\n" + "\t\tswitch (mode) {\n" + "\t\tcase UNNECESSARY:\n"
				+ "\t\t\tcheckRoundingUnnecessary(x == floorPow);\n" + "\t\t\t// fall through\n" + "\t\tcase DOWN:\n"
				+ "\t\t\tresult = logFloor;\n" + "\t\tcase CEILING:\n" + "\t\tcase UP:\n"
				+ "\t\t\tresult = (x == floorPow) ? logFloor : logFloor - 1;\n" + "\t\tcase HALF_DOWN:\n"
				+ "\t\tcase HALF_UP:\n" + "\t\tcase HALF_EVEN:\n"
				+ "\t\t\t// sqrt(10) is irrational, so log10(x) - logFloor is never exactly\n" + "\t\t\t// 0.5\n"
				+ "\t\t\tresult = (x <= HALF_POWERS_OF_10[logFloor]) ? logFloor : logFloor - 1;\n" + "\t\t}\n"
				+ "\t\treturn result;\n" + "\t}\n" + "\n" + "\tprivate static int log10Floor(int x) {\n"
				+ "\t\tint y = MAX_LOG_10_FOR_LEADING_ZEROS[Integer.numberOfLeadingZeros(x)];\n"
				+ "\t\tint sgn = (x - POWERS_OF_10[y]) >>> (Integer.SIZE - 1);\n" + "\t\treturn y - sgn;\n" + "\t}\n"
				+ "\n" + "\tstatic void checkRoundingUnnecessary(boolean condition) {\n" + "\t\tif (!condition) {\n"
				+ "\t\t\tthrow new ArithmeticException(\"mode was UNNECESSARY, but rounding was necessary\");\n"
				+ "\t\t}\n" + "\t}\n" + "\n" + "\tstatic int checkPositive(String role, int x) {\n"
				+ "\t\tif (x <= 0) {\n"
				+ "\t\t\tthrow new IllegalArgumentException(role + \" (\" + x + \") must be > 0\");\n" + "\t\t}\n"
				+ "\t\treturn x;\n" + "\t}\n" + "}\n";
	}
}
