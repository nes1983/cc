package ch.unibe.scg.cc;

import java.nio.ByteBuffer;

import ch.unibe.scg.cc.Protos.Snippet;

import com.google.common.collect.ImmutableMultimap;

public class PopularSnippetMaps {
	/**
	 * Maps from functions hashes to all of their popular snippet. See the
	 * popularsnippets Htable definition for details.
	 */
	final private ImmutableMultimap<ByteBuffer, Snippet> function2PopularSnippets;
	/** Maps from snippet hash to popular snippet locations. */
	final private ImmutableMultimap<ByteBuffer, Snippet> snippet2PopularSnippets;

	public PopularSnippetMaps(ImmutableMultimap<ByteBuffer, Snippet> function2PopularSnippets,
			ImmutableMultimap<ByteBuffer, Snippet> snippet2PopularSnippets) {
		this.function2PopularSnippets = function2PopularSnippets;
		this.snippet2PopularSnippets = snippet2PopularSnippets;
	}

	public ImmutableMultimap<ByteBuffer, Snippet> getFunction2PopularSnippets() {
		return function2PopularSnippets;
	}

	public ImmutableMultimap<ByteBuffer, Snippet> getSnippet2PopularSnippets() {
		return snippet2PopularSnippets;
	}
}
