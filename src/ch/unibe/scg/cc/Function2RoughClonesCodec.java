package ch.unibe.scg.cc;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.Protos.SnippetMatch;

import com.google.protobuf.ByteString;

class Function2RoughClonesCodec implements Codec<SnippetMatch> {
	@Override
	public Cell<SnippetMatch> encode(SnippetMatch m) {
		ByteString colKey = ByteString.copyFrom(Bytes.add(m.getThatSnippetLocation().getFunction().toByteArray(),
				Bytes.toBytes(m.getThisSnippetLocation().getPosition()),
				Bytes.toBytes(m.getThisSnippetLocation().getLength())));
		return new Cell<>(m.getThisSnippetLocation().getFunction(), colKey, m.toByteString());
	}

	@Override
	public SnippetMatch decode(Cell<SnippetMatch> encoded) throws IOException {
		return SnippetMatch.parseFrom(encoded.cellContents);
	}
}
