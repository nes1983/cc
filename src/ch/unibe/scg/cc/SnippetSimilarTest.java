package ch.unibe.scg.cc;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.javaFrontend.JavaType1ReplacerFactory;
import ch.unibe.scg.cc.lines.StringOfLines;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;
import ch.unibe.scg.cc.mappers.Protos.SnippetLocation;
import ch.unibe.scg.cc.mappers.Protos.SnippetMatch;
import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.protobuf.ByteString;

/**
 * Test {@link CloneExpander}.
 * @author Niko Schwarz
 */
@SuppressWarnings("javadoc")
public final class SnippetSimilarTest {
	static class SnippetMatchComparator implements Comparator<SnippetMatch> {
		@Override public int compare(SnippetMatch o1, SnippetMatch o2) {
			return Integer.compare(o1.getThisSnippetLocation().getPosition(),
					o2.getThatSnippetLocation().getPosition());
		}
	}

	static class SnippetLocationComparator implements Comparator<SnippetLocation> {
		@Override public int compare(SnippetLocation o1, SnippetLocation o2) {
			return o1.getSnippet().asReadOnlyByteBuffer().compareTo(
					o2.getSnippet().asReadOnlyByteBuffer());
		}
	}

	final Comparator<SnippetMatch> snippetMatchComparator = new SnippetMatchComparator();
	final Comparator<SnippetLocation> snippetLocationComparator = new SnippetLocationComparator();

	@Test
	public void testAreSimilar() throws CannotBeHashedException {
		final Normalizer n1 = new Normalizer(new JavaType1ReplacerFactory().get());
		final Normalizer n2 = new Normalizer(new Type2ReplacerFactory().get());
		final ShingleHasher ss = Guice.createInjector(new CCModule(), new JavaModule())
				.getInstance(ShingleHasher.class);

		final StringBuilder s1 = snippet1();
		final StringBuilder s2 = snippet2();

		// type-1 normalization
		n1.normalize(s1);
		n1.normalize(s2);
		// type-2 normalization
		n2.normalize(s1);
		n2.normalize(s2);

		final StringOfLinesFactory solFactory = new StringOfLinesFactory();

		final StringOfLines sol1 = solFactory.make(s1.toString());
		final StringOfLines sol2 = solFactory.make(s2.toString());

		final List<SnippetLocation> hashes1 = allSnippets(sol1, ss, new byte[] { 1 });
		final List<SnippetLocation> hashes2 = allSnippets(sol2, ss, new byte[] { 2 });

		final List<SnippetMatch> matches = Lists.newArrayList();
		for (final SnippetLocation e : hashes1) {
			final int pos = Collections.binarySearch(hashes2, e, snippetLocationComparator);
			if (pos >= 0) {
				matches.add(SnippetMatch.newBuilder()
						.setThisSnippetLocation(e)
						.setThatSnippetLocation(hashes2.get(pos))
						.build());
			}
		}
		Collections.sort(matches, snippetMatchComparator);

		// Extract matches
		final Collection<Clone> builtClones = new CloneExpander().expandClones(matches);
		assertThat(builtClones.toString(), is("[this_function: \"\\001\"\n" +
				"that_function: \"\\002\"\n" +
				"this_from_position: 5\n" +
				"that_from_position: 3\n" +
				"this_length: 17\n" +
				"that_length: 17\n" +
				"]"));
	}

	ImmutableList<SnippetLocation> allSnippets(StringOfLines sol, Hasher hasher, byte[] function) throws CannotBeHashedException {
		final List<SnippetLocation> ret = Lists.newArrayList();

		for (int frameStart = 0; frameStart + CloneExpander.MINIMUM_LINES
				< sol.getNumberOfLines(); frameStart++) {
			final String value = sol.getLines(frameStart, CloneExpander.MINIMUM_LINES);
			final ByteBuffer newHash = ByteBuffer.wrap(hasher.hash(value));
			ret.add(SnippetLocation.newBuilder()
					.setSnippet(ByteString.copyFrom(newHash))
					.setFunction(ByteString.copyFrom(function))
					.setPosition(frameStart)
					.build());
		}

		Collections.sort(ret, snippetLocationComparator);
		return ImmutableList.copyOf(ret); // compresses ret.
	}

	StringBuilder snippet1() {
		return new StringBuilder("	public static int log10(int x, RoundingMode mode) {\n"
				+ "		checkPositive(\"x\", x);\n"
				+ "		int logFloor = log10Floor(x);\n"
				+ "		int floorPow = POWERS_OF_10[logFloor];\n"
				+ "		int result = -1;\n"
				+ "		switch (mode) {\n"
				+ "		case UNNECESSARY:\n"
				+ "			checkRoundingUnnecessary(x == floorPow);\n"
				+ "			// fall through\n"
				+ "		case DOWN:\n"
				+ "			result = logFloor;\n"
				+ "		case CEILING:\n" + "		case UP:\n"
				+ "			result = (x == floorPow) ? logFloor : logFloor - 1;\n"
				+ "		case HALF_DOWN:\n"
				+ "		case HALF_UP:\n"
				+ "		case HALF_EVEN:\n"
				+ "			// sqrt(10) is irrational, so log10(x) - logFloor is never exactly\n"
				+ "			// 0.5\n"
				+ "			result = (x <= HALF_POWERS_OF_10[logFloor]) ? logFloor : logFloor - 1;\n"
				+ "		}\n"
				+ "		return result;\n"
				+ "	}");
	}

	StringBuilder snippet2() {
		return new StringBuilder("	public static int log10(int x, RoundingMode mode) {\n"
				+ "		int logFloor = log10Floor(x);\n"
				+ "		int floorPow = powers_Of_10[logFloor];\n"
				+ "		switch (mode) {\n"
				+ "		case UNNECESSARY:\n"
				+ "			checkRoundingUnnecessary(x == floorPow);\n"
				+ "			// fall through\n"
				+ "		case FLOOR:\n"
				+ "		case DOWN:\n"
				+ "			return logFloor;\n"
				+ "		case CEILING:\n"
				+ "		case UP:\n"
				+ "			return (x == floorPow) ? logFloor : logFloor + 1;\n"
				+ "		case HALF_DOWN:\n"
				+ "		case HALF_UP:\n"
				+ "		case HALF_EVEN:\n"
				+ "			// sqrt(10) is irrational, so log10(x) - logFloor is never exactly\n"
				+ "			// 0.5\n"
				+ "			return (x <= half_Powers_Of_10[logFloor]) ? logFloor : logFloor + 1;\n"
				+ "		default:\n"
				+ "			throw new AssertionError();\n"
				+ "		}\n"
				+ "	}");
	}
}