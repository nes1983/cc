package ch.unibe.scg.cc;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.Codec;

import com.google.protobuf.ByteString;

class Function2RoughClonesCodec implements Codec<Clone> {
	private static final long serialVersionUID = 1L;

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
