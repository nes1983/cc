package ch.unibe.scg.cc;

import com.google.protobuf.ByteString;

/**
 * An entry of the strings table for type T. For example, the string for a
 * function of hash h would be stored in a cell like
 * {@code new Str<Function>(h, contents);}.
 *
 * @param <T>
 *            The type of the underlying proto buffer. For example, the strings of
 *            a clone are stored in type Str<Clone>.
 */
class Str<T> {
	final ByteString hash;
	final String contents;

	Str(ByteString hash, String content) {
		this.hash = hash;
		this.contents = content;
	}
}
