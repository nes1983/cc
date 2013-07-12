package ch.unibe.scg.cc.lines;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class StringOfLinesTest {
	final static String sampleString = "a\n" + "b\n" + "c\n" + "d\n" + "e\n" + "f\n" + "g\n";

	@Test
	public void testLinesOfCode() {
		StringOfLines stringOfLines = new StringOfLinesFactory().make(sampleString);
		assertThat(stringOfLines.getNumberOfLines(), is(7));
		assertThat(new StringOfLinesFactory().make("a").getNumberOfLines(), is(1));
		assertThat(stringOfLines.getLines(3, 3), is("\nd\ne\nf\n"));
	}

	/** Testing {@link StringOfLines#toString()} */
	@Test
	public void testToString() {
		assertThat(new StringOfLinesFactory().make(sampleString).toString(), is(sampleString));
	}
}
