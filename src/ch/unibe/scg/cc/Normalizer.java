package ch.unibe.scg.cc;

import java.io.Serializable;

/** A normalizer normalizes a StringBuilder such that two similar strings normalize to the same one */
public interface Normalizer extends Serializable {
	/** Normalize similar contents to the same string. */
	void normalize(StringBuilder contents);
}