package ch.unibe.scg.cc.mappers;

import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.Protos.SnippetLocation;
import ch.unibe.scg.cc.Protos.SnippetLocationOrBuilder;

import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;

class PopularSnippetsCodec {
	static byte[] encodeRowKey(SnippetLocationOrBuilder loc) {
		return loc.getFunction().toByteArray();
	}

	static byte[] encodeColumnKey(SnippetLocationOrBuilder loc) {
		return loc.getSnippet().toByteArray();
	}

	static byte[] encodeCellValue(SnippetLocationOrBuilder loc) {
		return Bytes.add(Bytes.toBytes(loc.getPosition()), Bytes.toBytes(loc.getLength()));
	}

	static SnippetLocation decodeSnippetLocation(byte[] rowKey, byte[] columnKey, byte[] columnValue) {
		return SnippetLocation.newBuilder()
				.setFunction(ByteString.copyFrom(rowKey))
				.setSnippet(ByteString.copyFrom(columnKey))
				.setPosition(Bytes.toInt(Bytes.head(columnValue, Ints.BYTES)))
				.setLength(Bytes.toInt(Bytes.tail(columnValue, Ints.BYTES))).build();
	}
}
