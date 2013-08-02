package ch.unibe.scg.cells;

import java.io.IOException;

/** Wraps an IOException that happened during decoding */
class EncodingException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	EncodingException(IOException wrapped) {
		super(wrapped);
	}

	/** Same as {@link #getCause()} which is guaranteed to be an IOException */
	IOException getIOException() {
		return (IOException) getCause();
	}
}
