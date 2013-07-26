package ch.unibe.scg.cc;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import org.unibe.scg.cells.Mapper;

import ch.unibe.scg.cc.Annotations.CloneLoader;
import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;

import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;

/** Expand clones and filter clones down to the clones that aren't spam. */
class Function2FineCloner implements Mapper<Clone, Clone> {
	final private CloneExpander cloneExpander;
	final private LoadingCache<byte[], String> cloneLoader;
	final private SpamDetector spamDetector;
	final private StringOfLinesFactory stringOfLinesFactory;
	final private Logger logger;

	@Inject
	Function2FineCloner(CloneExpander cloneExpander, @CloneLoader LoadingCache<byte[], String> cloneLoader,
			SpamDetector spamDetector, StringOfLinesFactory stringOfLinesFactory, Logger logger) {
		this.cloneExpander = cloneExpander;
		this.cloneLoader = cloneLoader;
		this.spamDetector = spamDetector;
		this.stringOfLinesFactory = stringOfLinesFactory;
		this.logger = logger;
	}

	@Override
	public void map(Clone first, Iterable<Clone> row, Sink<Clone> sink) throws IOException {
		Collection<Clone> clones = cloneExpander.expandClones(row);
		for (Clone c : clones) {
			try {
				// TODO: Counters are missing.
				if (!spamDetector.isSpamByParameters(spamDetector.extractFeatureVector(
						stringOfLinesFactory.make(cloneLoader.get(c.getThisSnippet().getFunction().toByteArray()))
								.getLines(c.getThisSnippet().getPosition(), c.getThisSnippet().getLength()),
						stringOfLinesFactory.make(cloneLoader.get(c.getThatSnippet().getFunction().toByteArray()))
								.getLines(c.getThatSnippet().getPosition(), c.getThatSnippet().getLength())))) {
					sink.write(c);
				}
			} catch (ExecutionException e) {
				Throwables.propagateIfPossible(e.getCause(), IOException.class);
				logger.severe("Failure while trying to load sources for " + c + e.getCause());
			}
		}
	}

	@Override
	public void close() { }
}
