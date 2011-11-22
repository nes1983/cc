package ch.unibe.scg.cc;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;

import ch.unibe.jexample.Given;
import ch.unibe.jexample.JExample;
import ch.unibe.scg.cc.regex.Replace;

@RunWith(JExample.class)
public class Type2ReplacerFactoryTest {

	@Test
	public PhaseFrontent testFactory() {
		Type2ReplacerFactory factory = new Type2ReplacerFactory();
		Replace[] replaces = factory.get();
		assertThat(replaces, is(arrayWithSize(3)));
		Normalizer normalizer = new Normalizer(replaces);
		return normalizer;
	}
	
	@Given("testFactory")
	public PhaseFrontent testNormalize(PhaseFrontent phase) {
		StringBuilder sb = new StringBuilder(sampleString());
		phase.normalize(sb);
		assertThat(sb.toString(), is("\t\tt. t.t. t(t t[] { 1, 1 ,1 });\n\t}\n"));
		return phase;
	}
	
	String sampleString() {
		return "\t\tfish.stink.Rod.doIt(new int[] { 1, 2 ,3 });\n" + "	}\n";
	}

}
