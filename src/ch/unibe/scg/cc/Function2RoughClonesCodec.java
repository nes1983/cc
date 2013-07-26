package ch.unibe.scg.cc;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.unibe.scg.cells.Cell;
import org.unibe.scg.cells.Codec;

import ch.unibe.scg.cc.Protos.Clone;

import com.google.protobuf.ByteString;

class Function2RoughClonesCodec implements Codec<Clone> {
	@Override
	public Cell<Clone> encode(Clone m) {
		ByteString colKey = ByteString.copyFrom(Bytes.add(m.getThatSnippet().getFunction().toByteArray(),
				Bytes.toBytes(m.getThisSnippet().getPosition()),
				Bytes.toBytes(m.getThisSnippet().getLength())));
		return Cell.make(m.getThisSnippet().getFunction(), colKey, m.toByteString());
	}

	@Override
	public Clone decode(Cell<Clone> encoded) throws IOException {
		return Clone.parseFrom(encoded.getCellContents());
	}
}
