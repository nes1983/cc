package ch.unibe.scg.cc;

import java.nio.ByteBuffer;

import ch.unibe.scg.cc.Protos.SnippetLocation;

import com.google.common.collect.ImmutableMultimap;

public class PopularSnippetMaps {
	/**
	 * Maps from functions hashes to all of their popular snippet. See the
	 * popularsnippets Htable definition for details.
	 */
	final ImmutableMultimap<ByteBuffer, SnippetLocation> function2PopularSnippets;
	/** Maps from snippet hash to popular snippet locations. */
	final ImmutableMultimap<ByteBuffer, SnippetLocation> snippet2PopularSnippets;

	public PopularSnippetMaps(ImmutableMultimap<ByteBuffer, SnippetLocation> function2PopularSnippets,
			ImmutableMultimap<ByteBuffer, SnippetLocation> snippet2PopularSnippets) {
		this.function2PopularSnippets = function2PopularSnippets;
		this.snippet2PopularSnippets = snippet2PopularSnippets;
	}

	public ImmutableMultimap<ByteBuffer, SnippetLocation> getFunction2PopularSnippets() {
		return function2PopularSnippets;
	}

	public ImmutableMultimap<ByteBuffer, SnippetLocation> getSnippet2PopularSnippets() {
		return snippet2PopularSnippets;
	}
}
