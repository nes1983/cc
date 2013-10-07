package ch.unibe.scg.cc;


import java.io.Serializable;

import ch.unibe.scg.cc.Protos.CodeFile;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Protos.Project;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.Protos.Version;
import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.Codec;

import com.google.common.base.Charsets;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/** Translates from Cell to our protobufs and back */
class PopulatorCodec implements Serializable {
	private static final long serialVersionUID = 1L;

	final Codec<CodeFile> codeFile = new CodeFileCodec();
	/** Maps from functions to snippets. */
	final Codec<Snippet> snippet = new Function2SnippetCodec();
	final Codec<Version> version = new VersionCodec();
	final Codec<Function> function = new FunctionCodec();
	final Codec<Project> project = new ProjectCodec();

	static class CodeFileCodec implements Codec<CodeFile> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<CodeFile> encode(CodeFile fil) {
			return Cell.make(fil.getVersion(),
					ByteString.copyFrom(Bytes.concat(fil.getHash().toByteArray(), fil.getPath().getBytes(Charsets.UTF_8))),
					fil.toByteString());
		}

		@Override
		public CodeFile decode(Cell<CodeFile> encoded) throws InvalidProtocolBufferException {
			return CodeFile.parseFrom(encoded.getCellContents());
		}
	}

	static class Function2SnippetCodec implements Codec<Snippet> {
		private static final long serialVersionUID = 1L;

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
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<Version> encode(Version v) {
			return Cell.make(v.getProject(),
					ByteString.copyFrom(Bytes.concat(v.getHash().toByteArray(), v.getName().getBytes(Charsets.UTF_8))),
					v.toByteString());

		}

		@Override
		public Version decode(Cell<Version> encoded) throws InvalidProtocolBufferException {
			return Version.parseFrom(encoded.getCellContents());
		}
	}

	static class FunctionCodec implements Codec<Function> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<Function> encode(Function fun) {
			ByteString colKey = ByteString.copyFrom(Bytes.concat(fun.getHash().toByteArray(),
					Ints.toByteArray(fun.getBaseLine())));

			return Cell.make(fun.getCodeFile(), colKey, fun.toByteString());
		}

		@Override
		public Function decode(Cell<Function> encoded) throws InvalidProtocolBufferException {
			return Function.parseFrom(encoded.getCellContents());
		}
	}

	static class ProjectCodec implements Codec<Project> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<Project> encode(Project project) {
			return Cell.make(project.getHash(), project.getHash(), project.toByteString());
		}

		@Override
		public Project decode(Cell<Project> encoded) throws InvalidProtocolBufferException {
			return Project.parseFrom(encoded.getCellContents());
		}
	}
}
