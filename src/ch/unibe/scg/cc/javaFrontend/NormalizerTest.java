package ch.unibe.scg.cc.javaFrontend;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import ch.unibe.scg.cc.ReplacerNormalizer;

@SuppressWarnings("javadoc")
public final class NormalizerTest {
	@Test
	public void testNormalizer() {
		ReplacerNormalizer n = new ReplacerNormalizer(new JavaType1ReplacerFactory().get());
		StringBuilder sb = new StringBuilder("\npublic    static void doIt(char[] arg) {\n");
		n.normalize(sb);
		assertThat(sb.toString(), is("\nstatic void doIt(char[] arg) {\n"));
	}
}
