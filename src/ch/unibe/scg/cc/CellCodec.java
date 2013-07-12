package ch.unibe.scg.cc;

import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.Protos.CodeFile;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Protos.Project;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.Protos.Version;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/** Translates from Cell to our protobufs and back */
// TODO: Decode.
class CellCodec {
	Cell<CodeFile> encodeCodeFile(CodeFile fil) {
		return new Cell<>(fil.getVersion(), fil.getPathBytes(), fil.toByteString());
	}

	CodeFile decodeCodeFile(Cell<CodeFile> encoded) throws InvalidProtocolBufferException {
		return CodeFile.parseFrom(encoded.cellContents);
	}

	Cell<Snippet> encodeSnippet(Snippet snip) {
		byte[] colKey = Bytes.add(Bytes.toBytes((byte) snip.getCloneType().getNumber()),
				Bytes.toBytes(snip.getPosition()));

		return new Cell<>(snip.getFunction(), ByteString.copyFrom(colKey), snip.toByteString());
	}

	Snippet decodeSnippet(Cell<Snippet> encoded) throws InvalidProtocolBufferException {
		return Snippet.parseFrom(encoded.cellContents);
	}

	Cell<Version> encodeVersion(Version v) {
		return new Cell<>(v.getProject(), v.getNameBytes(), v.toByteString());
	}

	Version decodeVersion(Cell<Version> encoded) throws InvalidProtocolBufferException {
		return Version.parseFrom(encoded.cellContents);
	}

	Cell<Function> encodeFunction(Function fun) {
		ByteString colKey = ByteString.copyFrom(Bytes.toBytes(fun.getBaseLine()));

		return new Cell<>(fun.getCodeFile(), colKey, fun.toByteString());
	}

	Function decodeFunction(Cell<Function> encoded) throws InvalidProtocolBufferException {
		return Function.parseFrom(encoded.cellContents);
	}

	Cell<Project> encodeProject(Project project) {
		return new Cell<>(project.getHash(), ByteString.EMPTY, project.toByteString());
	}

	Project decodeProject(Cell<Project> encoded) throws InvalidProtocolBufferException {
		return Project.parseFrom(encoded.cellContents);
	}
}
