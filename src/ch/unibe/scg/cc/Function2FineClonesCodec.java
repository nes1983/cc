package ch.unibe.scg.cc;

import java.io.IOException;

import org.unibe.scg.cells.Cell;
import org.unibe.scg.cells.Codec;

import ch.unibe.scg.cc.Protos.CloneGroup;

import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

class Function2FineClonesCodec implements Codec<CloneGroup> {
	@Override
	public Cell<CloneGroup> encode(CloneGroup cg) {
		return Cell.make(ByteString.copyFrom(Hashing.sha1().hashString(cg.getText()).asBytes()),
				ByteString.EMPTY,
				cg.toByteString());
	}

	@Override
	public CloneGroup decode(Cell<CloneGroup> encoded) throws IOException {
		return CloneGroup.parseFrom(encoded.getCellContents());
	}
}
