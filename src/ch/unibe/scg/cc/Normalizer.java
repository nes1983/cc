package ch.unibe.scg.cc;

/** A normalizer normalizes a StringBuilder such that two similar strings normalize to the same one */
public interface Normalizer {
	/** Normalize similar contents to the same string. */
	void normalize(StringBuilder contents);
}