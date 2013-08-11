package ch.unibe.scg.cc;

import java.io.IOException;
import java.util.Collection;

import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;
import ch.unibe.scg.cells.LookupTable;
import ch.unibe.scg.cells.Mapper;
import ch.unibe.scg.cells.Sink;

import com.google.common.collect.Iterables;
import com.google.inject.Inject;

/** Expand clones and filter clones down to the clones that aren't spam. */
class Function2FineCloner implements Mapper<Clone, Clone> {
	private static final long serialVersionUID = 1L;

	final private CloneExpander cloneExpander;
	final private LookupTable<Str<Function>> cloneLoader;
	final private SpamDetector spamDetector;
	final private StringOfLinesFactory stringOfLinesFactory;

	@Inject
	Function2FineCloner(StringOfLinesFactory stringOfLinesFactory, CloneExpander cloneExpander,
			@Annotations.Populator LookupTable<Str<Function>> cloneLoader, SpamDetector spamDetector) {
		this.stringOfLinesFactory = stringOfLinesFactory;
		this.cloneExpander = cloneExpander;
		this.cloneLoader = cloneLoader;
		this.spamDetector = spamDetector;
	}

	@Override
	public void map(Clone first, Iterable<Clone> row, Sink<Clone> sink) throws IOException, InterruptedException {
		first.getThisSnippet().getFunction();

		Collection<Clone> clones = cloneExpander.expandClones(row);
		for (Clone c : clones) {
			// TODO: Counters are missing.
			if (spamDetector.isSpamByParameters(spamDetector.extractFeatureVector(
					stringOfLinesFactory.make(
							Iterables.getOnlyElement(cloneLoader.readRow(c.getThisSnippet().getFunction())).contents)
							.getLines(c.getThisSnippet().getPosition(), c.getThisSnippet().getLength()),
					stringOfLinesFactory.make(
							Iterables.getOnlyElement(cloneLoader.readRow(c.getThatSnippet().getFunction())).contents)
							.getLines(c.getThatSnippet().getPosition(), c.getThatSnippet().getLength())))) {
				return;
			}

			sink.write(c);
		}
	}



	@Override
	public void close() throws IOException {
		if (cloneLoader != null) {
			cloneLoader.close();
		}
	}

}
