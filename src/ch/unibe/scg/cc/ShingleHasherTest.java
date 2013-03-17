package ch.unibe.scg.cc;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;

import ch.unibe.jexample.Given;
import ch.unibe.jexample.JExample;
import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;

@RunWith(JExample.class)
public class ShingleHasherTest {

	@Test
	public ShingleHasher test() throws CannotBeHashedException {
		ShingleHasher ss = Guice.createInjector(new CCModule(), new JavaModule()).getInstance(ShingleHasher.class);

		String[] shingles = ss.shingles("one two three four five six seven eight nine");
		assertThat(shingles, is(new String[] { "one two three four", "five six seven eight", "two three four five",
				"six seven eight nine", "three four five six", "four five six seven" }));

		assertThat(((Collection) ss.hashedShingles(shingles)).size(), is(6));

		return ss;
	}

	@Given("test")
	public ShingleHasher testEntireSketch(ShingleHasher ss) throws CannotBeHashedException {
		byte[] sketch = ss.hash("one two three four five six");
		assertThat(Arrays.toString(sketch), startsWith("[68, -114,"));
		return ss;
	}

	@Test
	public void testStrangeInput() throws CannotBeHashedException {
		ShingleHasher ss = Guice.createInjector(new CCModule(), new JavaModule()).getInstance(ShingleHasher.class);
		ss.hash("} t (t t) { t. t(); t. t(1); } } }");
		ss.hash("t t; t t; t t; t t; t t;");
		ss.hash("} t t(t t) { t (t. t()) {");
	}
}
