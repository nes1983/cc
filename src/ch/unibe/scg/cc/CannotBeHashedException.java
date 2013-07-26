package ch.unibe.scg.cc;

/** Hashers can refuse to hash a snippet. In that case, the snippet should not be used. */
public class CannotBeHashedException extends Exception {
	private static final long serialVersionUID = 1L;
}
