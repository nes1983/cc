package ch.unibe.scg.cc;

import java.io.Serializable;
import java.util.Comparator;

import ch.unibe.scg.cc.Protos.Clone;

import com.google.common.collect.ComparisonChain;

public class CloneComparator implements Comparator<Clone>, Serializable {
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