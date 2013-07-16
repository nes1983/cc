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
class PopulatorCodec {
	final Codec<CodeFile> codeFile = new CodeFileCodec();
	/** Maps from functions to snippets. */
	final Codec<Snippet> snippet = new Function2SnippetCodec();
	final Codec<Version> version = new VersionCodec();
	final Codec<Function> function = new FunctionCodec();
	final Codec<Project> project = new ProjectCodec();

	private static class CodeFileCodec implements Codec<CodeFile> {
		@Override
		public Cell<CodeFile> encode(CodeFile fil) {
			return new Cell<>(fil.getVersion(), fil.getPathBytes(), fil.toByteString());
		}

		@Override
		public CodeFile decode(Cell<CodeFile> encoded) throws InvalidProtocolBufferException {
			return CodeFile.parseFrom(encoded.cellContents);
		}
	}

	private static class Function2SnippetCodec implements Codec<Snippet> {
		@Override
		public Cell<Snippet> encode(Snippet snip) {
			byte[] colKey = Bytes.add(Bytes.toBytes((byte) snip.getCloneType().getNumber()),
					Bytes.toBytes(snip.getPosition()));

			return new Cell<>(snip.getFunction(), ByteString.copyFrom(colKey), snip.toByteString());
		}

		@Override
		public Snippet decode(Cell<Snippet> encoded) throws InvalidProtocolBufferException {
			return Snippet.parseFrom(encoded.cellContents);
		}

	}

	private static class VersionCodec implements Codec<Version> {
		@Override
		public Cell<Version> encode(Version v) {
			return new Cell<>(v.getProject(), v.getNameBytes(), v.toByteString());
		}

		@Override
		public Version decode(Cell<Version> encoded) throws InvalidProtocolBufferException {
			return Version.parseFrom(encoded.cellContents);
		}
	}

	private static class FunctionCodec implements Codec<Function> {
		@Override
		public Cell<Function> encode(Function fun) {
			ByteString colKey = ByteString.copyFrom(Bytes.toBytes(fun.getBaseLine()));

			return new Cell<>(fun.getCodeFile(), colKey, fun.toByteString());
		}

		@Override
		public Function decode(Cell<Function> encoded) throws InvalidProtocolBufferException {
			return Function.parseFrom(encoded.cellContents);
		}
	}

	private static class ProjectCodec implements Codec<Project> {
		@Override
		public Cell<Project> encode(Project project) {
			return new Cell<>(project.getHash(), ByteString.EMPTY, project.toByteString());
		}

		@Override
		public Project decode(Cell<Project> encoded) throws InvalidProtocolBufferException {
			return Project.parseFrom(encoded.cellContents);
		}
	}
}
