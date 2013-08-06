package ch.unibe.scg.cc;

import java.io.IOException;

import ch.unibe.scg.cc.Protos.CloneGroup;
import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.Codec;

import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

class CloneGroupCodec implements Codec<CloneGroup> {
	final private ByteString columnName = ByteString.copyFromUtf8("cloneGroup");

	@Override
	public Cell<CloneGroup> encode(CloneGroup cg) {
		return Cell.make(ByteString.copyFrom(Hashing.sha1().hashString(cg.getText()).asBytes()),
				columnName,
				cg.toByteString());
	}

	@Override
	public CloneGroup decode(Cell<CloneGroup> encoded) throws IOException {
		return CloneGroup.parseFrom(encoded.getCellContents());
	}
}
