package ch.unibe.scg.cc;

import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collection;

import org.hamcrest.core.Is;
import org.junit.Test;

import ch.unibe.scg.cc.javaFrontend.JavaModule;

import com.google.common.collect.Iterables;
import com.google.inject.Guice;

@SuppressWarnings("javadoc")
public class ShingleHasherTest {
	@Test
	public void test() throws CannotBeHashedException {
		ShingleHasher ss = Guice.createInjector(new CCModule(), new InMemoryModule(), new JavaModule()).getInstance(ShingleHasher.class);

		Collection<String> shingles = ss.shingles("one two three four five six seven eight nine");
		assertThat(shingles, Is.<Collection<String>>is(Arrays.asList("one two three four", "five six seven eight", "two three four five",
				"six seven eight nine", "three four five six", "four five six seven")));

		assertThat(Iterables.size(ss.hashedShingles(shingles)), is(6));

		assertThat(Arrays.toString(ss.hash("one two three four five six")), startsWith("[68, -114,"));
	}

	@Test
	public void testStrangeInput() throws CannotBeHashedException {
		ShingleHasher ss = Guice.createInjector(new CCModule(), new InMemoryModule(), new JavaModule()).getInstance(ShingleHasher.class);
		ss.hash("} t (t t) { t. t(); t. t(1); } } }");
		ss.hash("t t; t t; t t; t t; t t;");
		ss.hash("} t t(t t) { t (t. t()) {");
	}
}
