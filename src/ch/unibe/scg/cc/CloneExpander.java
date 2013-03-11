package ch.unibe.scg.cc;

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
	static final int MINIMUM_LINES = 5;

	public Collection<Clone> expandClones(final List<SnippetMatch> matches) {
		final ImmutableList.Builder<Clone> clones = ImmutableList.builder();
		final List<SnippetMatch> unprocessedMatches = Lists.newLinkedList(matches);
		while (!unprocessedMatches.isEmpty()) {
			final Clone.Builder clone = Clone.newBuilder();
			final Iterator<SnippetMatch> iter = unprocessedMatches.iterator();
			SnippetMatch last = iter.next();
			iter.remove();

			clone.setThisFunction(last.getThisSnippetLocation().getFunction());
			clone.setThatFunction(last.getThatSnippetLocation().getFunction());
			clone.setThisFromPosition(last.getThisSnippetLocation().getPosition());
			clone.setThatFromPosition(last.getThatSnippetLocation().getPosition());
			clone.setThisLength(1);
			clone.setThatLength(1);

			while (iter.hasNext()) {
				final SnippetMatch cur = iter.next();
				if (!cur.getThatSnippetLocation().getFunction().equals(last.getThatSnippetLocation().getFunction())) {
					throw new RuntimeException("to be implemented.");
				}
				if (Math.abs(last.getThisSnippetLocation().getPosition()
						- cur.getThisSnippetLocation().getPosition()) <= MAX_GAP) {
					if (Math.abs(last.getThatSnippetLocation().getPosition()
							- cur.getThatSnippetLocation().getPosition()) <= MAX_GAP) {
						iter.remove();

						clone.setThisLength(
								cur.getThatSnippetLocation().getPosition() - clone.getThatFromPosition() + 1);
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
			clone.setThisLength(clone.getThisLength() + MINIMUM_LINES - 1);
			clone.setThatLength(clone.getThatLength() + MINIMUM_LINES - 1);
			clones.add(clone.build());
		}
		final Collection<Clone> builtClones = clones.build();
		return builtClones;
	}
}
