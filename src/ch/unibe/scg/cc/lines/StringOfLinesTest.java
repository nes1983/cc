package ch.unibe.scg.cc.lines;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import ch.unibe.jexample.Given;
import ch.unibe.jexample.JExample;
import ch.unibe.scg.cc.javaFrontend.JavaTokenizer;

@RunWith(JExample.class)
public class StringOfLinesTest {

	final String sampleString = "a\n" + "b\n" + "c\n" + "d\n" + "e\n" + "f\n"
			+ "g\n";

	@Test
	public StringOfLines testLinesOfCode() {
		StringOfLines stringOfLines = new StringOfLinesFactory()
				.make(sampleString);
		assertThat(stringOfLines.getNumberOfLines(), is(7));
		assertThat(new StringOfLinesFactory().make("a").getNumberOfLines(), is(1));
		return stringOfLines;
	}
	

	@Given("testLinesOfCode")
	public void getLines(StringOfLines stringOfLines) {
		assertThat(stringOfLines.getLines(3, 3), is("\nd\ne\nf\n"));
	}
 
}




