package ch.unibe.scg.cc;

import org.unibe.scg.cells.Cell;
import org.unibe.scg.cells.Codec;

import ch.unibe.scg.cc.Protos.Snippet;

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
