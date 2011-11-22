package ch.unibe.scg.cc.regex;

import static org.hamcrest.Matchers.*;

import java.util.ArrayList;

import jregex.Pattern;

import static org.junit.Assert.*;

import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;

import ch.unibe.jexample.Given;
import ch.unibe.jexample.JExample;
import ch.unibe.scg.cc.regex.ReplacementString.LiteralSegment;
import ch.unibe.scg.cc.regex.ReplacementString.PlaceHolderSegment;
import ch.unibe.scg.cc.regex.ReplacementString.Segment;
import dk.brics.automaton.RegExp;

@RunWith(JExample.class)
public class ReplaceTest {

	@Test
	public Replace testMakeReplace() {
		Replace r = new Replace(new Pattern("x"), "y");
		assertThat(r.replacementString.contents.size(), is(1));
		return r;
		
	}
	
	@Given("testMakeReplace")
	public Replace testReplace(Replace r) {
		StringBuilder sb = new StringBuilder("123x123");
		
		r.replaceAll(sb);
		assertThat(sb.toString(), is("123y123"));
		return r;
	}
	
	@Test 
	public Replace makePatternReplace() {
		Replace r = new Replace(new Pattern("x"), "<$0>");
		ArrayList<Segment> contents = r.replacementString.contents;
		assertThat(contents.size(), is(3));
		assertThat(contents.get(0), ((Matcher) isA(LiteralSegment.class)));
		assertThat(contents.get(1), ((Matcher) isA(PlaceHolderSegment.class)));
		assertThat(contents.get(2), ((Matcher) isA(LiteralSegment.class)));
		assertThat(((LiteralSegment)contents.get(2)).s, is(">"));
		return r;
	}
	
	
	@Given("makePatternReplace")
	public Replace useReplacePattern(Replace r) {
		StringBuilder sb = new StringBuilder("xxyxx");
		r.replaceAll(sb);
		assertThat(sb.toString(), is("<x><x>y<x><x>"));
		return r;
	}
	
	@Test 
	public Replace simpleSelfReplace() {
		Replace r = new Replace(new Pattern("(x)"), "$0");
		String selfReplaced = r.allReplaced("xxx");
		assertThat(selfReplaced, is("xxx"));
		return r;
	}
	
	@Test 
	public Replace reverseThings() {
		Replace r = new Replace(new Pattern("(\\d*)(bla)"), "$2$1");

		ArrayList<Segment> contents = r.replacementString.contents;


		String reversed = r.allReplaced("123bla");
		assertThat(reversed, is("bla123"));
		return r;
	}

}
