package ch.unibe.scg.cc;


import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.Codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

class FunctionStringCodec implements Codec<Str<Function>> {
	@Override
	public Cell<Str<Function>> encode(Str<Function> s) {
		return Cell.make(
				s.hash,
				ByteString.EMPTY,
				ByteString.copyFromUtf8(s.contents));
	}

	@Override
	public Str<Function> decode(Cell<Str<Function>> encoded) throws InvalidProtocolBufferException {
		return new Str<>(encoded.getRowKey(), encoded.getCellContents().toStringUtf8());
	}
}
