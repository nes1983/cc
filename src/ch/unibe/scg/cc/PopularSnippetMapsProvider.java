package ch.unibe.scg.cc;

import java.nio.ByteBuffer;

import javax.inject.Provider;


import ch.unibe.scg.cc.Annotations.PopularSnippets;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cells.Source;

import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;

class PopularSnippetMapsProvider implements Provider<PopularSnippetMaps> {
	final private Source<Snippet> src;

	@Inject
	PopularSnippetMapsProvider(@PopularSnippets Source<Snippet> src) {
		this.src = src;
	}

	// Must be static to force sharing across all JVMs.
	private static PopularSnippetMaps popularSnippetMaps;

	@Override
	public PopularSnippetMaps get() {
		synchronized (PopularSnippetMapsProvider.class) {
			if (popularSnippetMaps == null) {
				// TODO: Replace ByteBuffer with ByteString, like we do in the rest of the project.
				ImmutableMultimap.Builder<ByteBuffer, Snippet> function2PopularSnippets = ImmutableMultimap.builder();
				ImmutableMultimap.Builder<ByteBuffer, Snippet> snippet2PopularSnippets = ImmutableMultimap.builder();

				for (Iterable<Snippet> row : src) {
					for (Snippet snip : row) {
						function2PopularSnippets.put(
								snip.getFunction().asReadOnlyByteBuffer(), snip);
						snippet2PopularSnippets.put(snip.getHash().asReadOnlyByteBuffer(), snip);
					}
				}

				popularSnippetMaps = new PopularSnippetMaps(function2PopularSnippets.build(),
						snippet2PopularSnippets.build());
			}

			return popularSnippetMaps;
		}
	}
}