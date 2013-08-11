package ch.unibe.scg.cc.regex;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.regex.MatchResult;

import org.junit.Test;

import ch.unibe.scg.cc.regex.ReplacementString.LiteralSegment;
import ch.unibe.scg.cc.regex.ReplacementString.PlaceHolderSegment;
import ch.unibe.scg.cc.regex.ReplacementString.Segment;

@SuppressWarnings("javadoc")
public final class ReplacementStringTest {

	@Test
	public void testSimpleConstruction() {
		ReplacementString s = new ReplacementString("waa$2blaa$1");
		List<Segment> contents = s.contents;

		assertEquals(LiteralSegment.class, contents.get(0).getClass());
		assertEquals(PlaceHolderSegment.class, contents.get(1).getClass());
		assertEquals(LiteralSegment.class, contents.get(2).getClass());
		assertEquals(PlaceHolderSegment.class, contents.get(3).getClass());

		assertThat(((LiteralSegment) contents.get(0)).s, is("waa"));
		assertThat(((PlaceHolderSegment) contents.get(1)).placeHolderNumber, is(2));
		assertThat(((LiteralSegment) contents.get(2)).s, is("blaa"));
		assertThat(((PlaceHolderSegment) contents.get(3)).placeHolderNumber, is(1));
	}

	@Test
	public void testConstruction() {
		ReplacementString s = new ReplacementString("$2$1");
		List<Segment> contents = s.contents;

		assertEquals(PlaceHolderSegment.class, contents.get(0).getClass());
		assertEquals(PlaceHolderSegment.class, contents.get(1).getClass());

		assertThat(((PlaceHolderSegment) contents.get(0)).placeHolderNumber, is(2));
		assertThat(((PlaceHolderSegment) contents.get(1)).placeHolderNumber, is(1));

		MatchResult matchResult = mock(MatchResult.class);
		when(matchResult.group(1)).thenReturn("a");
		when(matchResult.group(2)).thenReturn("b");

		assertThat(s.fillIn(matchResult), is("ba"));
	}
}
