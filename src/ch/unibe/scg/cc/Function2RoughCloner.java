package ch.unibe.scg.cc;

import java.io.IOException;
import java.util.Collection;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.unibe.scg.cc.Annotations.PopularSnippets;
import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.Snippet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;

/** Filter phase of the pipeline */
class Function2RoughCloner implements Mapper<Snippet, Clone> {
	private static final int POPULAR_SNIPPET_THRESHOLD = 500;

	final private CellSink<Snippet> popularSnippets;

	final private Codec<Snippet> popularSnippetsCodec;
	final private Logger logger;

	@Inject
	Function2RoughCloner(@PopularSnippets CellSink<Snippet> popularSnippets,
			@PopularSnippets Codec<Snippet> popularSnippetsCodec, Logger logger) {
		this.popularSnippets = popularSnippets;
		this.popularSnippetsCodec = popularSnippetsCodec;
		this.logger = logger;
	}

	/** Input encoding: snippet2function */
	@Override
	public void map(Snippet first, Iterable<Snippet> rowIterable, Sink<Clone> function2RoughClones) throws IOException {
		// rowIterable is not guaranteed to be iterable more than once, so copy.
		Collection<Snippet> row = ImmutableList.copyOf(rowIterable);
		rowIterable = null; // Don't touch!

		logger.finer("Make rough clone from snippet "
				+ BaseEncoding.base16().encode(Iterables.get(row, 0).getHash().toByteArray()).substring(0, 6));

		if (row.size() <= 1) {
			return; // prevent processing non-recurring hashes
		}

		// special handling of popular snippets
		if (row.size() > POPULAR_SNIPPET_THRESHOLD) {
			for (Snippet loc : row) {
				popularSnippets.write(popularSnippetsCodec.encode(loc));
			}
			return;
		}

		for (Snippet thisSnip : row) {
			for (Snippet thatSnip : row) {
				// save only half of the functions as row-key
				// full table gets reconstructed in MakeSnippet2FineClones
				// This *must* be the same as in CloneExpander.
				if (thisSnip.getFunction().asReadOnlyByteBuffer()
						.compareTo(thatSnip.getFunction().asReadOnlyByteBuffer()) >= 0) {
					continue;
				}

				function2RoughClones
						.write(Clone.newBuilder().setThisSnippet(thisSnip).setThatSnippet(thatSnip).build());
			}
		}
	}

	@Override
	public void close() throws IOException {
		popularSnippets.close();
	}
}
