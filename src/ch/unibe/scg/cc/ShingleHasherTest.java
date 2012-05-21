package ch.unibe.scg.cc;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import org.apache.commons.lang3.ArrayUtils;
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
		ShingleHasher ss = Guice.createInjector(new CCModule(), new JavaModule()).getInstance(
				ShingleHasher.class);

		String[] shingles = ss
				.shingles("one two three four five six seven eight nine");
		assertThat(shingles, is(new String[] { "one two three four",
				"five six seven eight", "two three four five",
				"six seven eight nine", "three four five six",
				"four five six seven" }));

		byte[][] hashedShingles = ss.hashedShingles(shingles);
		assertThat(hashedShingles, is(arrayWithSize(6)));

		return ss;

	}

	@Given("test")
	public ShingleHasher testEntireSketch(ShingleHasher ss) throws CannotBeHashedException {
		byte[] sketch = ss.hash("one two three four five six");
		assertThat(ArrayUtils.toString(sketch), startsWith("{20,57,")); 
		// The first two check out.
		// That's the XOR of the following two.
		// {-125,62,35,96,122,41,-101,74,-113,-121,80,36,-5,-86,-10,15,-56,61,26,-55}
		// {-105,7,-7,62,-107,65,-74,-6,54,104,33,-58,-103,-42,5,-91,-59,-69,-23,107}
		return ss;
	}
}
