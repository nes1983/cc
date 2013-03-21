package ch.unibe.scg.cc;

public class WrappedRuntimeException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public WrappedRuntimeException(Throwable cause) {
		super(cause);
	}

	public WrappedRuntimeException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
