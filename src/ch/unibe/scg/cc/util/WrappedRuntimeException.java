package ch.unibe.scg.cc.util;

public class WrappedRuntimeException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public WrappedRuntimeException(Throwable cause) {
		super(cause);
	}
}
