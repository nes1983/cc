package ch.unibe.scg.cc;

import ch.unibe.scg.cc.mappers.GuiceResource;

/**
 * Every Hadoop counter is named by an enum. These are ours. See {@link Counter}
 * . The Counters' string values are stored in {@link GuiceResource} to satisfy
 * Guice seeing a constant expression.
 */
public enum Counters {
	/** Number of times a {@link CannotBeHashedException} was thrown. */
	CANNOT_BE_HASHED,
	/** Number of times the hashing operation was successful. */
	SUCCESSFULLY_HASHED;
}