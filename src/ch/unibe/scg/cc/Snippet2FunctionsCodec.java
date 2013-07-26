package ch.unibe.scg.cc;

import org.unibe.scg.cells.Cell;
import org.unibe.scg.cells.Codec;

import ch.unibe.scg.cc.Protos.Snippet;

import com.google.protobuf.InvalidProtocolBufferException;

class Snippet2FunctionsCodec implements Codec<Snippet> {
	@Override
	public Cell<Snippet> encode(Snippet s) {
		// We assume only one occurrence of a snippet inside a single function
		return Cell.make(s.getHash(), s.getFunction(), s.toByteString());
	}

	@Override
	public Snippet decode(Cell<Snippet> encoded) throws InvalidProtocolBufferException {
		return Snippet.parseFrom(encoded.getCellContents());
	}
}
