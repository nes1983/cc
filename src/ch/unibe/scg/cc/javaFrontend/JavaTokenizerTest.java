package ch.unibe.scg.cc.javaFrontend;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;

import java.util.List;

import javax.inject.Provider;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import ch.unibe.scg.cc.activerecord.Function;
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
		Answer<Object> a = Mockito.RETURNS_DEEP_STUBS;

		JavaTokenizer tokenizer = new JavaTokenizer();
		Provider<Function> provider = (Provider<Function>) Mockito
				.mock(Provider.class);
		Function function = Mockito.mock(Function.class);
		stub(provider.get()).toReturn(function);
		tokenizer.functionProvider = provider;

		List<Function> list = tokenizer.tokenize(sampleClass(), null);
		// Don't delete! The only way to fix the unit test!
		// for( Map.Entry<String, Location> e : m.entrySet()) {
		// logger.debug(StringEscapeUtils.escapeJava(e.getKey()));
		// logger.debug("---");
		// }

		assertThat(list.size(), is(4));
		verify(function)
				.setContents(
						"package        fish.stink;\nimport java.util.*;\nimport static System.out;\n\n");
		verify(function).setContents("public class Rod {\n\t");
		verify(function)
				.setContents(
						"public static void main(String[] args) {\n\t\tout.println(\"Hiya, wassup?\");\n\t\tfish.stink.Rod.doIt(new int[] { 1, 2 ,3 });\n\t}\n\t\t\t");
		verify(function)
				.setContents(
						"protected static void doIt() {\n\t\tif(System.timeInMillis() > 10000)\n\t\t\tout.println(1337);\n\t\tmain(null);\n\t}\n}\n");

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

	@Test
	public void testOtherClass() {
		JavaTokenizer tokenizer = new JavaTokenizer();
		Provider<Function> provider = (Provider<Function>) Mockito
				.mock(Provider.class);
		Function function = Mockito.mock(Function.class);
		stub(provider.get()).toReturn(function);
		tokenizer.functionProvider = provider;

		List<Function> list = tokenizer.tokenize(otherClass(), null);
		// Don't delete! The only way to fix the unit test!
		// for( Map.Entry<String, Location> e : m.entrySet()) {
		// logger.debug(StringEscapeUtils.escapeJava(e.getKey()));
		// logger.debug("---");
		// }

		assertThat(list.size(), is(3));
		verify(function).setContents("package ch.unibe.testdata;\n\n");
		verify(function).setContents(
				"public class Apfel {\n\n\t/**\n\t * @param args\n\t */\n\t");
		verify(function).setContents(
				"public static void main(String[] args) {\n"
						+ "\t\tString name = \"Apfel\";\n"
						+ "\t\tSystem.out.println(name);\n"
						+ "\t\tint a = 10;\n" + "\t\twhile (a > 0) {\n"
						+ "\t\t\tSystem.out.println(\"a\" + a);\n"
						+ "\t\t\ta -= 1;\n" + "\t\t}\n"
						+ "\t\tfor (int i = 0; i < 10; i++) {\n"
						+ "\t\t\tSystem.out.println(\"i\" + i);\n" + "\t\t}\n"
						+ "\t\tSystem.out.println(\"finished\");\n"
						+ "\t\treturn;\n" + "\t}\n" + "	\n" + "}\n");
	}

	// produced a bug earlier
	String otherClass() {
		return "package ch.unibe.testdata;\n" + "\n" + "public class Apfel {\n"
				+ "\n" + "\t/**\n" + "\t * @param args\n" + "\t */\n"
				+ "\tpublic static void main(String[] args) {\n"
				+ "\t\tString name = \"Apfel\";\n"
				+ "\t\tSystem.out.println(name);\n" + "\t\tint a = 10;\n"
				+ "\t\twhile (a > 0) {\n"
				+ "\t\t\tSystem.out.println(\"a\" + a);\n" + "\t\t\ta -= 1;\n"
				+ "\t\t}\n" + "\t\tfor (int i = 0; i < 10; i++) {\n"
				+ "\t\t\tSystem.out.println(\"i\" + i);\n" + "\t\t}\n"
				+ "\t\tSystem.out.println(\"finished\");\n" + "\t\treturn;\n"
				+ "\t}\n" + "	\n" + "}\n";
	}

}
