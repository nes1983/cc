package ch.unibe.scg.cc;

import java.io.IOException;

import ch.unibe.scg.cc.Protos.GitRepo;

import com.google.protobuf.ByteString;

class GitRepoCodec implements Codec<GitRepo> {
	@Override
	public Cell<GitRepo> encode(GitRepo s) {
		return new Cell<>(ByteString.copyFromUtf8(s.getProjectName()), ByteString.EMPTY, s.toByteString());
	}

	@Override
	public GitRepo decode(Cell<GitRepo> encoded) throws IOException {
		return GitRepo.parseFrom(encoded.getCellContents());
	}
}
