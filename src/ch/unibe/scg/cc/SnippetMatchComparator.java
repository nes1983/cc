package ch.unibe.scg.cc;

import java.util.Comparator;

import ch.unibe.scg.cc.mappers.Protos.SnippetMatch;

import com.google.common.collect.ComparisonChain;

class SnippetMatchComparator implements Comparator<SnippetMatch> {
	@Override
	public int compare(SnippetMatch o1, SnippetMatch o2) {
		return ComparisonChain
				.start()
				.compare(
						o1.getThatSnippetLocation().getFunction().asReadOnlyByteBuffer(),
						o2.getThatSnippetLocation().getFunction().asReadOnlyByteBuffer())
				.compare(
						o1.getThisSnippetLocation().getPosition(),
						o2.getThisSnippetLocation().getPosition())
				.result();
	}
}