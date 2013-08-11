package ch.unibe.scg.cc.regex;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.List;
import java.util.regex.Pattern;

import org.junit.Test;

import ch.unibe.scg.cc.regex.ReplacementString.LiteralSegment;
import ch.unibe.scg.cc.regex.ReplacementString.PlaceHolderSegment;
import ch.unibe.scg.cc.regex.ReplacementString.Segment;

@SuppressWarnings("javadoc")
public final class ReplaceTest {
	@Test
	public void testMakeReplace() {
		Replace r = new Replace(Pattern.compile("x"), "y");
		assertThat(r.replacementString.contents.size(), is(1));

		StringBuilder sb = new StringBuilder("123x123");
		r.replaceAll(sb);
		assertThat(sb.toString(), is("123y123"));
	}

	@Test
	public void makePatternReplace() {
		Replace r = new Replace(Pattern.compile("x"), "<$0>");
		List<Segment> contents = r.replacementString.contents;
		assertThat(contents.size(), is(3));

		assertEquals(LiteralSegment.class, contents.get(0).getClass());
		assertEquals(PlaceHolderSegment.class, contents.get(1).getClass());
		assertEquals(LiteralSegment.class, contents.get(2).getClass());

		assertThat(((LiteralSegment) contents.get(2)).s, is(">"));

		StringBuilder sb = new StringBuilder("xxyxx");
		r.replaceAll(sb);
		assertThat(sb.toString(), is("<x><x>y<x><x>"));
	}

	@Test
	public void simpleSelfReplace() {
		Replace r = new Replace(Pattern.compile("(x)"), "$0");
		String selfReplaced = r.allReplaced("xxx");
		assertThat(selfReplaced, is("xxx"));
	}

	@Test
	public void reverseThings() {
		Replace r = new Replace(Pattern.compile("(\\d*)(bla)"), "$2$1");

		List<Segment> contents = r.replacementString.contents;
		assertThat(contents.size(), is(2));

		String reversed = r.allReplaced("123bla");
		assertThat(reversed, is("bla123"));
	}
}
