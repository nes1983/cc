package ch.unibe.scg.cc.javaFrontend;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import ch.unibe.scg.cc.Normalizer;

@SuppressWarnings("javadoc")
public final class NormalizerTest {
	@Test
	public void testNormalizer() {
		Normalizer n = new Normalizer(new JavaType1ReplacerFactory().get());
		StringBuilder sb = new StringBuilder("\npublic    static void doIt(char[] arg) {\n");
		n.normalize(sb);
		assertThat(sb.toString(), is("\nstatic void doIt(char[] arg) {\n"));
	}
}
