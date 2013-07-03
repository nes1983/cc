package ch.unibe.scg.cc;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.nio.ByteBuffer;

import javax.inject.Provider;

import org.junit.Test;

import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.Protos.SnippetMatch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.protobuf.ByteString;

/** Test {@link CloneExpander} */
public final class CloneExpanderTest {
	/** Test {@link CloneExpander#expandClones} */
	@Test
	public void testExpandClones() {
		// Construct something like h1fun2@3 h2fun2@5 h3fun3@1 h4fun3@7
		Snippet thisFunction = Snippet.newBuilder().setFunction(ByteString.copyFromUtf8("fun1")).build();
		ImmutableList<SnippetMatch> toExpand = ImmutableList.of(
				SnippetMatch.newBuilder()
					.setThisSnippetLocation(Snippet.newBuilder(thisFunction)
							.setHash(ByteString.copyFromUtf8("h1"))
							.setPosition(2))
					.setThatSnippetLocation(Snippet.newBuilder()
							.setFunction(ByteString.copyFromUtf8("fun2"))
							.setHash(ByteString.copyFromUtf8("h1"))
							.setPosition(2))
					.build(),

					SnippetMatch.newBuilder()
					.setThisSnippetLocation(Snippet.newBuilder(thisFunction)
							.setHash(ByteString.copyFromUtf8("h2"))
							.setPosition(5))
					.setThatSnippetLocation(Snippet.newBuilder()
							.setFunction(ByteString.copyFromUtf8("fun2"))
							.setHash(ByteString.copyFromUtf8("h2"))
							.setPosition(5))
					.build(),

					SnippetMatch.newBuilder()
					.setThisSnippetLocation(Snippet.newBuilder(thisFunction)
							.setHash(ByteString.copyFromUtf8("h3"))
							.setPosition(1))
					.setThatSnippetLocation(Snippet.newBuilder()
							.setFunction(ByteString.copyFromUtf8("fun3"))
							.setHash(ByteString.copyFromUtf8("h3"))
							.setPosition(1))
					.build(),

					SnippetMatch.newBuilder()
					.setThisSnippetLocation(Snippet.newBuilder(thisFunction)
							.setHash(ByteString.copyFromUtf8("h4"))
							.setPosition(7))
					.setThatSnippetLocation(Snippet.newBuilder()
							.setFunction(ByteString.copyFromUtf8("fun3"))
							.setHash(ByteString.copyFromUtf8("h1"))
							.setPosition(7))
					.build());

		// Construct maps for one popular snippet h7 that occurs in two places: fun2@6, fun1@2
		final Snippet fun2Loc = Snippet.newBuilder()
				.setHash(ByteString.copyFromUtf8("h7"))
				.setPosition(6)
				.setFunction(ByteString.copyFromUtf8("fun2")).build();
		final Snippet fun1Loc = Snippet.newBuilder()
				.setHash(ByteString.copyFromUtf8("h7"))
				.setPosition(1)
				.setFunction(ByteString.copyFromUtf8("fun1")).build();
		final ImmutableListMultimap<ByteBuffer, Snippet> snippet2Popular = ImmutableListMultimap.of(
				fun1Loc.getHash().asReadOnlyByteBuffer(), fun1Loc,
				fun2Loc.getHash().asReadOnlyByteBuffer(), fun2Loc);
		final ImmutableListMultimap<ByteBuffer, Snippet> function2Popular = ImmutableListMultimap.of(
				fun1Loc.getFunction().asReadOnlyByteBuffer(), fun1Loc,
				fun2Loc.getFunction().asReadOnlyByteBuffer(), fun2Loc);

		CloneExpander expander = new CloneExpander(new Provider<PopularSnippetMaps>() {
			@Override
			public PopularSnippetMaps get() {
				return new PopularSnippetMaps(function2Popular, snippet2Popular);
			}
		}, new SnippetMatchComparator());
		assertThat(expander.expandClones(toExpand).toString(), is(
				"[this_function: \"fun1\"\nthat_function: \"fun2\"\n" +
				"this_from_position: 1\nthat_from_position: 2\nthis_length: 9\nthat_length: 8\n, " +
				"this_function: \"fun1\"\nthat_function: \"fun3\"\n" +
				"this_from_position: 1\nthat_from_position: 1\nthis_length: 11\nthat_length: 11\n]"));
		}
}
