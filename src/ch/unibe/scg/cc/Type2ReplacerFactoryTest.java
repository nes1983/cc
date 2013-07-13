package ch.unibe.scg.cc;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import ch.unibe.scg.cc.regex.Replace;

@SuppressWarnings("javadoc")
public final class Type2ReplacerFactoryTest {
	@Test
	public void testFactory() {
		Type2ReplacerFactory factory = new Type2ReplacerFactory();
		Replace[] replaces = factory.get();
		assertThat(replaces, is(arrayWithSize(4)));

		Normalizer phase = new Normalizer(replaces);

		StringBuilder sb = new StringBuilder("\t\tfish.stink.Rod.doIt(new int[] { 1, 2 ,3 });\n" + "	}\n");
		phase.normalize(sb);
		assertThat(sb.toString(), is("\t\tt. t. t. t(t t[] { 1, 1 ,1 });\n\t}\n"));
	}
}
