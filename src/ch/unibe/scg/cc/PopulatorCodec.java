package ch.unibe.scg.cc;

import org.unibe.scg.cells.Cell;
import org.unibe.scg.cells.Codec;

import ch.unibe.scg.cc.Protos.CodeFile;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Protos.Project;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.Protos.Version;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
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

	static class CodeFileCodec implements Codec<CodeFile> {
		@Override
		public Cell<CodeFile> encode(CodeFile fil) {
			return Cell.make(fil.getVersion(), fil.getHash().concat(fil.getPathBytes()), fil.toByteString());
		}

		@Override
		public CodeFile decode(Cell<CodeFile> encoded) throws InvalidProtocolBufferException {
			return CodeFile.parseFrom(encoded.getCellContents());
		}
	}

	private static class Function2SnippetCodec implements Codec<Snippet> {
		@Override
		public Cell<Snippet> encode(Snippet snip) {
			byte[] colKey = Bytes.concat(new byte[] {(byte) snip.getCloneType().getNumber()},
					Ints.toByteArray(snip.getPosition()));

			return Cell.make(snip.getFunction(), ByteString.copyFrom(colKey), snip.toByteString());
		}

		@Override
		public Snippet decode(Cell<Snippet> encoded) throws InvalidProtocolBufferException {
			return Snippet.parseFrom(encoded.getCellContents());
		}

	}

	static class VersionCodec implements Codec<Version> {
		@Override
		public Cell<Version> encode(Version v) {
			return Cell.make(v.getProject(), v.getHash().concat(v.getNameBytes()), v.toByteString());
		}

		@Override
		public Version decode(Cell<Version> encoded) throws InvalidProtocolBufferException {
			return Version.parseFrom(encoded.getCellContents());
		}
	}

	private static class FunctionCodec implements Codec<Function> {
		@Override
		public Cell<Function> encode(Function fun) {
			ByteString colKey = fun.getHash().concat(ByteString.copyFrom(Ints.toByteArray(fun.getBaseLine())));

			return Cell.make(fun.getCodeFile(), colKey, fun.toByteString());
		}

		@Override
		public Function decode(Cell<Function> encoded) throws InvalidProtocolBufferException {
			return Function.parseFrom(encoded.getCellContents());
		}
	}

	static class ProjectCodec implements Codec<Project> {
		@Override
		public Cell<Project> encode(Project project) {
			return Cell.make(project.getHash(), ByteString.EMPTY, project.toByteString());
		}

		@Override
		public Project decode(Cell<Project> encoded) throws InvalidProtocolBufferException {
			return Project.parseFrom(encoded.getCellContents());
		}
	}
}
