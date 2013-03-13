package ch.unibe.scg.cc;

import static ch.unibe.scg.cc.RegisterClonesBackend.MINIMUM_LINES;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.mappers.Protos.SnippetMatch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/** Given a list of matches, extracts all fully expanded clones */
class CloneExpander {
	private static final int MAX_GAP = 10;
	// TODO: this should not be a constant here.
	// Instead, look at the snippetlocations, they should contain their length.

	public Collection<Clone> expandClones(final List<SnippetMatch> matches) {
		final ImmutableList.Builder<Clone> clones = ImmutableList.builder();
		final List<SnippetMatch> unprocessedMatches = Lists.newLinkedList(matches);
		while (!unprocessedMatches.isEmpty()) {
			final Iterator<SnippetMatch> iter = unprocessedMatches.iterator();
			SnippetMatch last = iter.next();
			iter.remove();

			Clone.Builder clone = initializeClone(last);

			while (iter.hasNext()) {
				final SnippetMatch cur = iter.next();
				if (!cur.getThatSnippetLocation().getFunction()
						.equals(last.getThatSnippetLocation().getFunction())) {
					clones.add(finalizeClone(clone));
					iter.remove();
					clone = initializeClone(cur);
					last = cur;
					continue;
				}
				if (Math.abs(last.getThisSnippetLocation().getPosition()
						- cur.getThisSnippetLocation().getPosition()) <= MAX_GAP) {
					if (Math.abs(last.getThatSnippetLocation().getPosition()
							- cur.getThatSnippetLocation().getPosition()) <= MAX_GAP) {
						iter.remove();

						clone.setThisLength(
								cur.getThisSnippetLocation().getPosition() - clone.getThisFromPosition() + 1);
						clone.setThatFromPosition(Math.min(clone.getThatFromPosition(), cur.getThatSnippetLocation()
								.getPosition()));
						clone.setThatLength(Math.max(clone.getThatLength(), cur.getThatSnippetLocation().getPosition()
								- clone.getThatFromPosition() + 1));
					}
				} else {
					break;
				}
				last = cur;
			}
			clones.add(finalizeClone(clone));
		}
		final Collection<Clone> builtClones = clones.build();
		return builtClones;
	}

	private Clone finalizeClone(final Clone.Builder clone) {
		clone.setThisLength(clone.getThisLength() + MINIMUM_LINES - 1);
		clone.setThatLength(clone.getThatLength() + MINIMUM_LINES - 1);
		return clone.build();
	}

	private Clone.Builder initializeClone(SnippetMatch firstMatch) {
		final Clone.Builder clone = Clone.newBuilder();
		clone.setThisFunction(firstMatch.getThisSnippetLocation().getFunction());
		clone.setThatFunction(firstMatch.getThatSnippetLocation().getFunction());
		clone.setThisFromPosition(firstMatch.getThisSnippetLocation().getPosition());
		clone.setThatFromPosition(firstMatch.getThatSnippetLocation().getPosition());
		clone.setThisLength(1);
		clone.setThatLength(1);
		return clone;
	}
}
