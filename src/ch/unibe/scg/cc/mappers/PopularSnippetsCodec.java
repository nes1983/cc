package ch.unibe.scg.cc.mappers;

import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.Protos.SnippetOrBuilder;

import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;

class PopularSnippetsCodec {
	static byte[] encodeRowKey(SnippetOrBuilder loc) {
		return loc.getFunction().toByteArray();
	}

	static byte[] encodeColumnKey(SnippetOrBuilder loc) {
		return loc.getHash().toByteArray();
	}

	static byte[] encodeCellValue(SnippetOrBuilder loc) {
		return Bytes.add(Bytes.toBytes(loc.getPosition()), Bytes.toBytes(loc.getLength()));
	}

	static Snippet decodeSnippetLocation(byte[] rowKey, byte[] columnKey, byte[] columnValue) {
		return Snippet.newBuilder()
				.setFunction(ByteString.copyFrom(rowKey))
				.setHash(ByteString.copyFrom(columnKey))
				.setPosition(Bytes.toInt(Bytes.head(columnValue, Ints.BYTES)))
				.setLength(Bytes.toInt(Bytes.tail(columnValue, Ints.BYTES))).build();
	}
}
