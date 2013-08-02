package ch.unibe.scg.cc;


import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.Codec;

import com.google.protobuf.InvalidProtocolBufferException;

class PopularSnippetsCodec implements Codec<Snippet> {
	@Override
	public Cell<Snippet> encode(Snippet s) {
		return Cell.make(s.getFunction(), s.getHash(), s.toByteString());
	}

	@Override
	public Snippet decode(Cell<Snippet> encoded) throws InvalidProtocolBufferException {
		return Snippet.parseFrom(encoded.getCellContents());
	}
}
