package ch.unibe.scg.cc.regex;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;

import jregex.MatchResult;

import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Mockito.*;

import ch.unibe.jexample.Given;
import ch.unibe.jexample.JExample;
import ch.unibe.scg.cc.regex.ReplacementString.LiteralSegment;
import ch.unibe.scg.cc.regex.ReplacementString.PlaceHolderSegment;
import ch.unibe.scg.cc.regex.ReplacementString.Segment;

@RunWith(JExample.class)
public class ReplacementStringTest {

	@Test
	public ReplacementString testSimpleConstruction() {
		ReplacementString s = new ReplacementString("waa$2blaa$1");
		ArrayList<Segment> contents = s.contents;
		
		assertThat(contents.get(0), ((Matcher) isA(LiteralSegment.class)));
		assertThat(contents.get(1), ((Matcher) isA(PlaceHolderSegment.class)));
		assertThat(contents.get(2), ((Matcher) isA(LiteralSegment.class)));
		assertThat(contents.get(3), ((Matcher) isA(PlaceHolderSegment.class)));
		
		assertThat(((LiteralSegment)contents.get(0)).s, is("waa"));
		assertThat(((PlaceHolderSegment)contents.get(1)).placeHolderNumber, is(2));
		assertThat(((LiteralSegment)contents.get(2)).s, is("blaa"));
		assertThat(((PlaceHolderSegment)contents.get(3)).placeHolderNumber, is(1));
		return s;
		
		
	}
	
	
	@Test
	public ReplacementString testConstruction() {
		ReplacementString s = new ReplacementString("$2$1");
		ArrayList<Segment> contents = s.contents;
		
		assertThat(contents.get(0), ((Matcher) isA(PlaceHolderSegment.class)));
		assertThat(contents.get(1), ((Matcher) isA(PlaceHolderSegment.class)));
		
		assertThat(((PlaceHolderSegment)contents.get(0)).placeHolderNumber, is(2));
		assertThat(((PlaceHolderSegment)contents.get(1)).placeHolderNumber, is(1));
		return s;
		
		
	}
	
	@Given("testConstruction") 
	public ReplacementString testFillingIn(ReplacementString rs) {
		MatchResult matchResult = mock(MatchResult.class);
		when(matchResult.group(1)).thenReturn("a");
		when(matchResult.group(2)).thenReturn("b");

		assertThat(rs.fillIn(matchResult), is("ba"));	
		return rs;
	}
}
