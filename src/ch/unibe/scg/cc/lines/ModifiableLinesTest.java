package ch.unibe.scg.cc.lines;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("javadoc")
public final class ModifiableLinesTest {
	ModifiableLinesFactory f;
	StringBuilder sb;
	private ModifiableLines r;

	@Before
	public void setUp() {
		f = new ModifiableLinesFactory();
		sb = new StringBuilder("a\n" + "b\n" + "c\n" + "d\n" + "e\n" + "f\n");
		r = f.make(sb);
	}

	@Test
	public void testFactory() {
		Collection<Integer> lineBreakPositions = new ArrayList<>();
		for (LineBreak br : r.lineBreaks) {
			lineBreakPositions.add(br.getPosition());
		}

		Collection<Integer> weights = new ArrayList<>();
		for (LineBreak br : r.lineBreaks) {
			lineBreakPositions.add(br.getWeight());
		}

		assertThat(weights, contains(1, 1, 1, 1, 1, 1, 1));
	}

	@Test
	public void replace(ModifiableLines lines) {
		lines.replace(3, 4, "waa");
		assertThat(lines.toString(), is("a\nbwaac\nd\ne\nf\n"));

		Collection<Integer> lineBreakPositions = new ArrayList<>();
		for (LineBreak br : lines.lineBreaks) {
			lineBreakPositions.add(br.getPosition());
		}
		assertThat(lineBreakPositions, contains(0, 1, 7, 9, 11, 13));

		Collection<Integer> weights = new ArrayList<>();
		for (LineBreak br : lines.lineBreaks) {
			lineBreakPositions.add(br.getWeight());
		}
		assertThat(weights, contains(1, 1, 2, 1, 1, 1));
	}
}
