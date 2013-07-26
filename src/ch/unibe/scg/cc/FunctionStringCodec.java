package ch.unibe.scg.cc;

import org.unibe.scg.cells.Cell;
import org.unibe.scg.cells.Codec;

import ch.unibe.scg.cc.Protos.Function;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

class FunctionStringCodec implements Codec<Function> {	
	@Override
	public Cell<Function> encode(Function function) {
		return Cell.make(
				function.getHash(), 
				ByteString.EMPTY, 
				ByteString.copyFromUtf8(function.getContents()));
	}

	@Override
	public Function decode(Cell<Function> encoded) throws InvalidProtocolBufferException {
		return Function.newBuilder()
				.setContents(encoded.getCellContents().toStringUtf8())
				.build();
	}
}
