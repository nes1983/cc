package ch.unibe.scg.cc;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.javaFrontend.JavaType1ReplacerFactory;
import ch.unibe.scg.cc.lines.StringOfLines;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;
import ch.unibe.scg.cells.InMemoryStorage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Guice;
import com.google.protobuf.ByteString;

// @formatter:off
/**
 * Test {@link CloneExpander}.
 * @author Niko Schwarz
 */
@SuppressWarnings("javadoc")
public final class SnippetSimilarTest {
	final Comparator<Clone> cloneComparator = new CloneExpander.CloneComparator();
	final Comparator<Snippet> snippetLocationComparator = new SnippetLocationComparator();

	static class SnippetLocationComparator implements Comparator<Snippet>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override public int compare(Snippet o1, Snippet o2) {
			return o1.getHash().asReadOnlyByteBuffer().compareTo(
					o2.getHash().asReadOnlyByteBuffer());
		}
	}

	@Test
	public void testAreSimilar() throws CannotBeHashedException {
		// Not fully refactored, so we easily add println statements
		// to make an example for the paper.
		final ReplacerNormalizer n1 = new ReplacerNormalizer(new JavaType1ReplacerFactory().get());
		final ReplacerNormalizer n2 = new ReplacerNormalizer(new Type2ReplacerFactory().get());
		final ShingleHasher ss = Guice.createInjector(new CCModule(new InMemoryStorage()))
				.getInstance(ShingleHasher.class);

		final StringBuilder s1 = snippet1();
		final StringBuilder s2 = snippet2();
		final StringBuilder s3 = snippet3();

		// type-1 normalization
		n1.normalize(s1);
		n1.normalize(s2);
		n1.normalize(s3);
		// type-2 normalization
		n2.normalize(s1);
		n2.normalize(s2);
		n2.normalize(s3);

		final StringOfLinesFactory solFactory = StringOfLinesFactory.getInstance();
		List<List<Snippet>> table = ImmutableList.of(
				allSnippets(solFactory.make(s1.toString(), '\n'), ss, new byte[] { 1 }),
				allSnippets(solFactory.make(s2.toString(), '\n'), ss, new byte[] { 2 }),
				allSnippets(solFactory.make(s3.toString(), '\n'), ss, new byte[] { 3 }));

		table = filterCollisions(table);

		final List<List<Clone>> matches = new ArrayList<>();
		for (int i = 0; i < table.size(); i++) {
			matches.add(new ArrayList<Clone>());
			// only look at j > i to avoid outputting the same match twice.
			for (int j = i + 1; j < table.size(); j++) {
				for (final Snippet e : table.get(i)) {
					final int pos = Collections.binarySearch(table.get(j), e, snippetLocationComparator);
					if (pos >= 0) {
						matches.get(i).add(Clone.newBuilder()
								.setThisSnippet(e)
								.setThatSnippet(table.get(j).get(pos))
								.build());
					}
				}
			}
			Collections.sort(matches.get(matches.size() - 1), cloneComparator);
		}

		assertThat(matches.size(), is(3));
		assertThat(printString(matches.get(0)),
				is("(1-2|5$bb3e@3, 1-2|13$20a4@12, 1-2|14$6f45@13, 1-3|13$20a4@9, 1-3|14$6f45@10, )"));
		assertThat(printString(matches.get(1)), is("(2-3|0$9721@0, 2-3|7$a037@4, 2-3|8$fc06@5, " +
				"2-3|9$03d8@6, 2-3|10$4fe1@8, 2-3|11$4fe1@8, 2-3|12$20a4@9, 2-3|13$6f45@10, " +
				"2-3|14$bfea@11, 2-3|15$abb0@12, 2-3|16$d0af@13, 2-3|17$c751@14, )"));
		assertThat(printString(matches.get(2)), is("()"));

		// Extract matches
		final CloneExpander expanderWithoutLongRows = new CloneExpander(
				new PopularSnippetMaps(null) {
					final private static long serialVersionUID = 1L;

					@Override public ImmutableMultimap<ByteBuffer, Snippet> getFunction2PopularSnippets() {
						return ImmutableMultimap.of();
					}

					@Override public ImmutableMultimap<ByteBuffer, Snippet> getSnippet2PopularSnippets() {
						return ImmutableMultimap.of();
					}
				});
		Collection<Clone> builtClones = expanderWithoutLongRows.expandClones(matches.get(0));
		assertThat(builtClones.toString(),
				is("[thisSnippet {\n  function: \"\\001\"\n  position: 5\n  length: 14\n}\n" +
					"thatSnippet {\n  function: \"\\002\"\n  position: 3\n  length: 15\n}\n, " +
					"thisSnippet {\n  function: \"\\001\"\n  position: 13\n  length: 6\n}\n" +
					"thatSnippet {\n  function: \"\\003\"\n  position: 9\n  length: 6\n}\n]"));
		builtClones = expanderWithoutLongRows.expandClones(matches.get(1));
		assertThat(builtClones.toString(),
				is("[thisSnippet {\n  function: \"\\002\"\n  position: 0\n  length: 22\n}\n" +
					"thatSnippet {\n  function: \"\\003\"\n  position: 0\n  length: 19\n}\n]"));
		builtClones = expanderWithoutLongRows.expandClones(matches.get(2));
		assertThat(builtClones.toString(), is("[]"));
	}

	/** Filter to keep only snippets that collide. */
	private List<List<Snippet>> filterCollisions(List<List<Snippet>> table) {
		final List<Snippet> filtering = new ArrayList<>();
		for (final List<Snippet> subTable : table) {
			filtering.addAll(subTable);
		}
		Collections.sort(filtering, new Comparator<Snippet>() {
			@Override public int compare(Snippet o1, Snippet o2) {
				return o1.getHash().asReadOnlyByteBuffer().compareTo(
						o2.getHash().asReadOnlyByteBuffer());
			}});

		table = ImmutableList.<List<Snippet>>of(
				new ArrayList<Snippet>(), new ArrayList<Snippet>(), new ArrayList<Snippet>());
		Snippet last = filtering.get(0);
		Snippet next = filtering.get(1);
		if (last.getHash().asReadOnlyByteBuffer().equals(next.getHash().asReadOnlyByteBuffer())) {
			table.get(last.getFunction().asReadOnlyByteBuffer().get(0) - 1).add(last);
		}
		for (int i = 1; i < filtering.size() - 1; i++) {
			final Snippet cur = next;
			next = filtering.get(i + 1);
			if (cur.getHash().asReadOnlyByteBuffer().equals(last.getHash().asReadOnlyByteBuffer())
					|| cur.getHash().asReadOnlyByteBuffer().equals(next.getHash().asReadOnlyByteBuffer())) {
						table.get(cur.getFunction().asReadOnlyByteBuffer().get(0) - 1).add(cur);
					}
			last = cur;
		}
		final Snippet cur = filtering.get(filtering.size() - 1);
		if (cur.getHash().asReadOnlyByteBuffer().equals(last.getHash().asReadOnlyByteBuffer())) {
			table.get(cur.getFunction().asReadOnlyByteBuffer().get(0) - 1).add(cur);
		}
		assertThat(filtering.size(), is(51));
		assertThat(table.get(0).size() + table.get(1).size() + table.get(2).size(), is(32));

		for (final List<Snippet> subTable : table) {
			Collections.sort(subTable, snippetLocationComparator);
		}
		return table;
	}

	List<Snippet> allSnippets(StringOfLines sol, Hasher hasher, byte[] function)
			throws CannotBeHashedException {
		final List<Snippet> ret = new ArrayList<>();

		for (int frameStart = 0; frameStart + Populator.MINIMUM_LINES
				< sol.getNumberOfLines(); frameStart++) {
			final String value = sol.getLines(frameStart, Populator.MINIMUM_LINES);
			final ByteBuffer newHash = ByteBuffer.wrap(hasher.hash(value));
			ret.add(Snippet.newBuilder()
					.setHash(ByteString.copyFrom(newHash))
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
				+ "		case CEILING:\n"
				+ "		case UP:\n"
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

	StringBuilder snippet3() {
		return new StringBuilder("	public static int log10(int x, RoundingMode mode) {\n"
				+ "		int logFloor = log10Floor(x);\n"
				+ "		int floorPow = powers_Of_10[logFloor];\n"
				+ "		switch (mode) {\n"
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

	private String printString(Iterable<Clone> matches) {
		final StringBuilder ret = new StringBuilder("(");
			for (final Clone e : matches) {
				final ByteBuffer hash = e.getThisSnippet().getHash().asReadOnlyByteBuffer();
				ret.append(String.format("%d-%d|%d$%02x%02x@%d, ",
						e.getThisSnippet().getFunction().asReadOnlyByteBuffer().get(0),
						e.getThatSnippet().getFunction().asReadOnlyByteBuffer().get(0),
						e.getThisSnippet().getPosition(), hash.get(0), hash.get(1),
						e.getThatSnippet().getPosition()));
			}
			ret.append(')');
		return ret.toString();
	}
}
