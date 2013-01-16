package ch.unibe.scg.cc;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.junit.runner.RunWith;

import ch.unibe.jexample.JExample;
import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.javaFrontend.JavaType1ReplacerFactory;

@RunWith(JExample.class)
public class FrontendTest {

	PhaseFrontend phaseFrontend = mock(PhaseFrontend.class);
	Tokenizer tokenizer = mock(Tokenizer.class);

	@Test
	public void testType2() {
		Normalizer p1 = new Normalizer(new JavaType1ReplacerFactory().get());
		Normalizer p2 = new Normalizer(new Type2ReplacerFactory().get());

		Frontend frontend = new Frontend(null, null, p1, p2, null, null, null, null, null, null);
		String s = frontend.type2NormalForm("\npublic    static void doIt(char[] arg) {\n");

		assertThat(s, is("\nt t t(t[] t) {\n"));
	}

	@Test
	public Frontend testNormalize() {
		Frontend frontend = new Frontend(null, null, phaseFrontend, phaseFrontend, null, null, null, null, null, null);
		frontend.type1NormalForm("\npublic    static void doIt(char[] arg) {\n");
		verify(phaseFrontend).normalize(any(StringBuilder.class));
		return frontend;
	}

	@Test
	public void testTokenizer() {
		Frontend frontend = new Frontend(null, null, phaseFrontend, null, null, null, tokenizer, null, null, null);
		CodeFile cf = mock(CodeFile.class);
		frontend.registerWithBuilder(new StringBuilder("\npublic    static void doIt(char[] arg) {\n"),
				"MickeyPaint.java", cf);
		verify(tokenizer).tokenize(eq("\npublic    static void doIt(char[] arg) {\n"), eq("MickeyPaint.java"));
	}

}
