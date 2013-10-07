package ch.unibe.scg.cc;

import java.io.Serializable;
import java.nio.ByteBuffer;

import ch.unibe.scg.cc.Annotations.PopularSnippets;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cells.Source;

import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;

/**
 * Loads the maps lazily. Lazy loading is useful when used from a serialized
 * class. It makes sure that loading only happens in the cluster.
 */
class PopularSnippetMaps implements Serializable {
	private static final long serialVersionUID = 1L;

	final private Source<Snippet> src;
	/**
	 * Maps from functions hashes to all of their popular snippet. See the
	 * popularsnippets Htable definition for details.
	 */
	private ImmutableMultimap<ByteBuffer, Snippet> function2PopularSnippets;
	/** Maps from snippet hash to popular snippet locations. */
	private ImmutableMultimap<ByteBuffer, Snippet> snippet2PopularSnippets;

	@Inject
	PopularSnippetMaps(@PopularSnippets Source<Snippet> src) {
		this.src = src;
	}

	public ImmutableMultimap<ByteBuffer, Snippet> getFunction2PopularSnippets() {
		load();
		return function2PopularSnippets;
	}

	public ImmutableMultimap<ByteBuffer, Snippet> getSnippet2PopularSnippets() {
		load();
		return snippet2PopularSnippets;
	}

	private synchronized void load() {
		if (function2PopularSnippets != null && snippet2PopularSnippets != null) {
			return;
		}
		// TODO: Replace ByteBuffer with ByteString, like we do in the rest of the project.
		ImmutableMultimap.Builder<ByteBuffer, Snippet> function2PopularSnippetsBuilder = ImmutableMultimap.builder();
		ImmutableMultimap.Builder<ByteBuffer, Snippet> snippet2PopularSnippetsBuilder = ImmutableMultimap.builder();

		for (Iterable<Snippet> row : src) {
			for (Snippet snip : row) {
				function2PopularSnippetsBuilder.put(snip.getFunction().asReadOnlyByteBuffer(), snip);
				snippet2PopularSnippetsBuilder.put(snip.getHash().asReadOnlyByteBuffer(), snip);
			}
		}

		function2PopularSnippets = function2PopularSnippetsBuilder.build();
		snippet2PopularSnippets = snippet2PopularSnippetsBuilder.build();
	}
}
