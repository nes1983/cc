package ch.unibe.scg.cc.javaFrontend;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Function.FunctionFactory;
import dk.brics.automaton.Automaton;
import dk.brics.automaton.AutomatonMatcher;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;

public class JavaTokenizerTest {

	@Test
	public void theRegexWorks() {
		String regex = new JavaTokenizer().splitterRegex;
		Automaton a = new RegExp(regex).toAutomaton();
		RunAutomaton r = new RunAutomaton(a);
		AutomatonMatcher m = r
				.newMatcher("\n      void main(String[] args) {   ");
		// assertTrue(m.find());

		assertFalse(r.newMatcher(
				"String myString = new String(new int[] { 1, 2, 3});").find());
		assertFalse(r.newMatcher("return Integer.class").find());
		assertTrue(r.newMatcher("\nclass MyClass<String> {").find());
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
		JavaTokenizer tokenizer = new JavaTokenizer();
		FunctionFactory functionFactory = Mockito.mock(FunctionFactory.class);
		tokenizer.functionFactory = functionFactory;

		List<Function> list = tokenizer.tokenize(sampleClass(), null);

		assertThat(list.size(), is(4));
		verify(functionFactory)
				.makeFunction(
						eq(0),
						eq("package        fish.stink;\nimport java.util.*;\nimport static System.out;\n\n"));
		verify(functionFactory).makeFunction(eq(4),
				eq("public class Rod {\n\t"));
		verify(functionFactory)
				.makeFunction(
						eq(5),
						eq("public static void main(String[] args) {\n\t\tout.println(\"Hiya, wassup?\");\n\t\tfish.stink.Rod.doIt(new int[] { 1, 2 ,3 });\n\t}\n\t\t\t"));
		verify(functionFactory)
				.makeFunction(
						eq(9),
						eq("protected static void doIt() {\n\t\tif(System.timeInMillis() > 10000)\n\t\t\tout.println(1337);\n\t\tmain(null);\n\t}\n}\n"));

	}

	String sampleClass() {
		return "package        fish.stink;\n" + "import java.util.*;\n"
				+ "import static System.out;\n\n" + "public class Rod {\n"
				+ "\tpublic static void main(String[] args) {\n"
				+ "\t\tout.println(\"Hiya, wassup?\");\n"
				+ "\t\tfish.stink.Rod.doIt(new int[] { 1, 2 ,3 });\n" + "	}\n"
				+ "\t\t\tprotected static void doIt() {\n"
				+ "\t\tif(System.timeInMillis() > 10000)\n"
				+ "\t\t\tout.println(1337);\n" + "\t\tmain(null);\n" + "\t}\n"
				+ "}\n";
	}
}
