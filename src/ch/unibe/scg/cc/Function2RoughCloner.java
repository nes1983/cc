package ch.unibe.scg.cc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

import javax.inject.Inject;

import ch.unibe.scg.cc.Annotations.Function2RoughClones;
import ch.unibe.scg.cc.Annotations.Function2Snippets;
import ch.unibe.scg.cc.Annotations.PopularSnippets;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.Protos.SnippetMatch;

import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;

/** Filter phase of the pipeline */
class Function2RoughCloner implements Mapper<Snippet, SnippetMatch>{
	private static final int POPULAR_SNIPPET_THRESHOLD = 500;

	final private CellSink<Snippet> popularSnippets;

	final private Codec<Snippet> function2SnippetsCodec;
	final private Codec<Snippet> popularSnippetsCodec;
	final private Codec<SnippetMatch> function2RoughClonesCodec;

	final private Logger logger;

	@Inject
	Function2RoughCloner(@PopularSnippets CellSink<Snippet> popularSnippets,
			@Function2Snippets Codec<Snippet> function2SnippetsCodec,
			@PopularSnippets Codec<Snippet> popularSnippetsCodec,
			@Function2RoughClones Codec<SnippetMatch> function2RoughClonesCodec, Logger logger) {
		this.popularSnippets = popularSnippets;
		this.function2SnippetsCodec = function2SnippetsCodec;
		this.popularSnippetsCodec = popularSnippetsCodec;
		this.function2RoughClonesCodec = function2RoughClonesCodec;
		this.logger = logger;
	}

	@Override
	public void map(Iterable<Cell<Snippet>> row, CellSink<SnippetMatch> function2RoughClones) throws IOException {
		logger.finer("Make rough clone "
				+ BaseEncoding.base16().encode(Iterables.get(row, 0).rowKey.toByteArray()).substring(0, 6));

		Collection<Snippet> locs = new ArrayList<>();
		for (Cell<Snippet> in : row) {
			locs.add(function2SnippetsCodec.decode(in));
		}

		if (locs.size() <= 1) {
			return; // prevent processing non-recurring hashes
		}

		// special handling of popular snippets
		if (locs.size() > POPULAR_SNIPPET_THRESHOLD) {
			for (Snippet loc : locs) {
				popularSnippets.write(popularSnippetsCodec.encode(loc));
			}
			return;
		}

		for (Snippet thisLoc : locs) {
			for (Snippet thatLoc : locs) {
				// save only half of the functions as row-key
				// full table gets reconstructed in MakeSnippet2FineClones
				// This *must* be the same as in CloneExpander.
				if (thisLoc.getFunction().asReadOnlyByteBuffer()
						.compareTo(thatLoc.getFunction().asReadOnlyByteBuffer()) >= 0) {
					continue;
				}

				//	REMARK 1: we don't set thisFunction because it gets
				//	already passed to the reducer as key. REMARK 2: we don't
				//	set thatSnippet because it gets already stored in
				//	thisSnippet
				function2RoughClones.write(function2RoughClonesCodec.encode(SnippetMatch.newBuilder()
						.setThisSnippetLocation(thisLoc).setThatSnippetLocation(thatLoc).build()));
			}
		}
	}
}
