package ch.unibe.scg.cc;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.io.Closer;

class ZippedGit implements Closeable {
	final InputStream packedRefs;
	final InputStream packFile;
	
	ZippedGit(InputStream packedRefs, InputStream packFile) {
		this.packedRefs = packedRefs;
		this.packFile = packFile;
	}

	@Override
	public void close() throws IOException {
		try(Closer closer = Closer.create()) {
			closer.register(packedRefs);
			closer.register(packFile);
		}
	}
}
