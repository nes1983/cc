package ch.unibe.scg.cc;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import javax.inject.Inject;
import javax.inject.Provider;

import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.CloneOrBuilder;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.Protos.Snippet.Builder;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Given a list of matches, extracts all fully expanded clones.
 *
 * <p>
 * Popular snippets are treated special. We enforce earlier in the pipeline that
 * all functions that contain popular snippets also appear at least once in a
 * match's {@link Clone#getThisSnippet()} as an input to
 * {@link #expandClones(Iterable)}.
 */
class CloneExpander {
	private static final int MAX_GAP = 10;
	// TODO: this should not be a constant here.
	// Instead, look at the snippets, they should contain their length.

	/**
	 * Maps from functions hashes to all of their popular snippet. See the
	 * popularsnippets Htable definition for details.
	 */
	private final ImmutableMultimap<ByteBuffer, Snippet> function2PopularSnippets;
	/** Maps from snippet hash to popular snippet locations. */
	private final ImmutableMultimap<ByteBuffer, Snippet> snippet2PopularSnippet;

	private final Comparator<Clone> cloneComparator = new CloneComparator();

	@Inject
	CloneExpander(Provider<PopularSnippetMaps> popularSnippetMapsProvider) {
		PopularSnippetMaps popularSnippetMaps = popularSnippetMapsProvider.get();
		this.function2PopularSnippets = popularSnippetMaps.getFunction2PopularSnippets();
		this.snippet2PopularSnippet = popularSnippetMaps.getSnippet2PopularSnippets();
	}

	static class CloneComparator implements Comparator<Clone>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(Clone o1, Clone o2) {
			return ComparisonChain
					.start()
					.compare(
							o1.getThatSnippet().getFunction().asReadOnlyByteBuffer(),
							o2.getThatSnippet().getFunction().asReadOnlyByteBuffer())
					.compare(
							o1.getThisSnippet().getPosition(),
							o2.getThisSnippet().getPosition())
					.result();
		}
	}

	/**
	 * Stitch together the matches into Clones.
	 *
	 * @param matches
	 *            sorted first by
	 *            {@code Clone.getThatSnippet().getFunction()},
	 *            second by
	 *            {@code Clone.getThisSnippet().getPosition()}.
	 *            {@code Clone.getThisSnippet.getFunction()} must
	 *            be constant for the entire list.
	 * @return The matches, stitched together.
	 */
	Collection<Clone> expandClones(final Iterable<Clone> matches) {
		if (Iterables.isEmpty(matches)) {
			return Collections.emptyList();
		}
		Clone first = matches.iterator().next();
		for (Clone match : matches) {
			assert match.getThisSnippet().getFunction().equals(first.getThisSnippet().getFunction());
		}

		final ImmutableList.Builder<Clone> clones = ImmutableList.builder();
		final LinkedList<Clone> unprocessedMatches = Lists.newLinkedList(matches);

		weaveInPopularSnippets(unprocessedMatches);

		while (!unprocessedMatches.isEmpty()) {
			final Iterator<Clone> iter = unprocessedMatches.iterator();
			Clone last = iter.next();
			iter.remove();

			Clone.Builder clone = initializeClone(last);

			while (iter.hasNext()) {
				final Clone cur = iter.next();
				if (!cur.getThatSnippet().getFunction().equals(last.getThatSnippet().getFunction())) {
					clones.add(finalizeClone(clone));
					iter.remove();
					clone = initializeClone(cur);
					last = cur;
					continue;
				}
				if (Math.abs(last.getThisSnippet().getPosition()
						- cur.getThisSnippet().getPosition()) <= MAX_GAP) {
					if (Math.abs(last.getThatSnippet().getPosition()
							- cur.getThatSnippet().getPosition()) <= MAX_GAP) {
						iter.remove();

						// Note that clones are sorted by thisLength, not thatLength.
						clone.getThisSnippetBuilder().setLength(cur.getThisSnippet().getPosition()
								- clone.getThisSnippetBuilder().getPosition() + 1);

						// Move position.
						Builder thatBuilder = clone.getThatSnippetBuilder();
						thatBuilder.setPosition(Math.min(thatBuilder.getPosition(), cur.getThatSnippet().getPosition()));
						thatBuilder.setLength(Math.max(thatBuilder.getLength(),
								cur.getThatSnippet().getPosition() - thatBuilder.getPosition() + 1));
					}
				} else {
					break;
				}
				last = cur;
			}
			clones.add(finalizeClone(clone));
		}
		return clones.build();
	}

	/** The unprocessed matches still lack the popular rows. Weave them in here. */
	private void weaveInPopularSnippets(LinkedList<Clone> unprocessedMatches) {
		if (unprocessedMatches.isEmpty()) {
			return;
		}

		final ByteBuffer thisFunction = unprocessedMatches.get(0).getThisSnippet().getFunction()
				.asReadOnlyByteBuffer();

		if (!function2PopularSnippets.containsKey(thisFunction)) {
			// Nothing to weave in.
			return;
		}

		List<Clone> toBeWeavedIns = new ArrayList<>();
		for (Snippet thisLocation : function2PopularSnippets.get(thisFunction)) {
			assert snippet2PopularSnippet.containsKey(thisLocation.getHash().asReadOnlyByteBuffer());
			for (Snippet thatLocation : snippet2PopularSnippet.get(thisLocation.getHash()
					.asReadOnlyByteBuffer())) {
				// The following three lines *must* match the test in
				// MakeFunction2RoughClones.java
				// The idea is that only clone a to b should be detected, not b
				// to a.
				if (thisFunction.compareTo(thatLocation.getFunction().asReadOnlyByteBuffer()) >= 0) {
					continue;
				}

				toBeWeavedIns.add(Clone.newBuilder().setThisSnippet(thisLocation)
						.setThatSnippet(thatLocation).build());
			}
		}

		Collections.sort(toBeWeavedIns, cloneComparator);

		final ListIterator<Clone> target = unprocessedMatches.listIterator();
		Clone cur = target.next();
		for (Clone toBeWeavedIn : toBeWeavedIns) {
			// Forward until we leave the right function or position is too big.
			while (target.hasNext()
					&& cloneComparator.compare(toBeWeavedIn, cur) > 0) {
				cur = target.next();
			}

			// We're one step too far now, so walk back left, unless the
			// insertion point is the last element
			if (cloneComparator.compare(toBeWeavedIn, cur) <= 0) {
				target.previous();
			}

			target.add(toBeWeavedIn);
		}
	}

	private Clone finalizeClone(final Clone.Builder clone) {
		clone.getThisSnippetBuilder().setLength(clone.getThisSnippet().getLength()
				+ Populator.MINIMUM_LINES - 1);
		clone.getThatSnippetBuilder().setLength(clone.getThatSnippet().getLength()
				+ Populator.MINIMUM_LINES - 1);
		return clone.build();
	}

	private Clone.Builder initializeClone(CloneOrBuilder firstMatch) {
		final Clone.Builder clone = Clone.newBuilder();
		Builder thisBuilder = clone.getThisSnippetBuilder();
		Builder thatBuilder = clone.getThatSnippetBuilder();

		thisBuilder.setFunction(firstMatch.getThisSnippet().getFunction());
		thatBuilder.setFunction(firstMatch.getThatSnippet().getFunction());

		thisBuilder.setPosition(firstMatch.getThisSnippet().getPosition());
		thatBuilder.setPosition(firstMatch.getThatSnippet().getPosition());

		thisBuilder.setLength(1);
		thatBuilder.setLength(1);
		return clone;
	}
}
