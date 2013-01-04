package ch.unibe.scg.cc;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import ch.unibe.scg.cc.javaFrontend.JavaType1ReplacerFactory;

public class NormalizerTest {
	@Test
	public void testNormalizer() {
		Normalizer n = new Normalizer(new JavaType1ReplacerFactory().get());
		StringBuilder sb = new StringBuilder(
				"\npublic    static void doIt(char[] arg) {\n");
		n.normalize(sb);
		assertThat(sb.toString(), is("\nstatic void doIt(char[] arg) {\n"));
	}

	@Test
	public void testNormalize2() {
		Normalizer n = new Normalizer(new Type2ReplacerFactory().get());
		StringBuilder sb = new StringBuilder(
				"\npublic    static void doIt(char[] arg) {\n");
		n.normalize(sb);
		assertThat(sb.toString(), is("\nt    t t t(t[] t) {\n"));
	}
}
