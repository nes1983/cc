package ch.unibe.scg.cc.javaFrontend;

import static org.junit.Assert.*;


import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.google.inject.Guice;

import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Location;
import ch.unibe.scg.cc.modules.CCModule;
import dk.brics.automaton.Automaton;
import dk.brics.automaton.AutomatonMatcher;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;
import static org.mockito.Mockito.*;
import javax.inject.Provider;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
public class JavaTokenizerTest {
	
	@Test
	public void theRegexWorks() {
		String regex = new JavaTokenizer().splitterRegex;
		Automaton a = new RegExp(regex).toAutomaton();
		RunAutomaton r = new RunAutomaton(a);
		AutomatonMatcher m = r.newMatcher("\n      void main(String[] args) {   ");
		//assertTrue(m.find());

		assertFalse(r.newMatcher("String myString = new String(new int[] { 1, 2, 3});").find());
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
	public void test() {
		Answer<Object> a = Mockito.RETURNS_DEEP_STUBS;
		
		JavaTokenizer tokenizer = new JavaTokenizer();
		Provider<Function> provider = (Provider<Function>) Mockito.mock(Provider.class);
		Function function = Mockito.mock(Function.class);
		stub(provider.get()).toReturn(function);
		tokenizer.functionProvider = provider;

		List<Function> list = tokenizer.tokenize(sampleClass(), null, null);
		//Don't delete! The only way to fix the unit test!
//		for( Map.Entry<String, Location> e : m.entrySet()) {
//			System.out.println(StringEscapeUtils.escapeJava(e.getKey()));
//			System.out.println("---");
//		}
		
		assertThat(list.size(), is(4));
		verify(function).setContents("package        fish.stink;\nimport java.util.*;\nimport static System.out;\n\n");
		verify(function).setContents("public class Rod {\n\t");
		verify(function).setContents("public static void main(String[] args) {\n\t\tout.println(\"Hiya, wassup?\");\n\t\tfish.stink.Rod.doIt(new int[] { 1, 2 ,3 });\n\t}\n\t\t\t");
		verify(function).setContents("protected static void doIt() {\n\t\tif(System.timeInMillis() > 10000)\n\t\t\tout.println(1337);\n\t\tmain(null);\n\t}\n}\n");
		
	}
	
	String sampleClass() {
		return    "package        fish.stink;\n" +
				  "import java.util.*;\n"
				+ "import static System.out;\n\n" 
				+ "public class Rod {\n"
				+ "\tpublic static void main(String[] args) {\n"
				+ "\t\tout.println(\"Hiya, wassup?\");\n"
				+ "\t\tfish.stink.Rod.doIt(new int[] { 1, 2 ,3 });\n" + "	}\n"
				+ "\t\t\tprotected static void doIt() {\n"
				+ "\t\tif(System.timeInMillis() > 10000)\n"
				+ "\t\t\tout.println(1337);\n" 
				+ "\t\tmain(null);\n"
				+ "\t}\n"
				+ "}\n";
	}

}
