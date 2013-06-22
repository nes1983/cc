package ch.unibe.scg.cc;

import ch.unibe.scg.cc.mappers.GuiceResource;
import ch.unibe.scg.cc.mappers.MakeFunction2FineClones.MakeFunction2FineClonesReducer;

/**
 * Every Hadoop counter is named by an enum. These are ours. See {@link org.apache.hadoop.mapreduce.Counters}.
 * The Counters' string values are stored in {@link GuiceResource} to satisfy
 * Guice seeing a constant expression.
 */
public enum Counters {
	/** Number of times a {@link CannotBeHashedException} was thrown. */
	CANNOT_BE_HASHED,
	/** Number of times the hashing operation was successful. */
	SUCCESSFULLY_HASHED,
	/** Number of processed java files. */
	PROCESSED_FILES,
	/** Number of ignored files (not processed). */
	IGNORED_FILES,
	/**
	 * Number of catched ArrayIndexOutOfBoundsException during reduce phase in
	 * {@link MakeFunction2FineClonesReducer}.
	 */
	MAKE_FUNCTION_2_FINE_CLONES_ARRAY_EXCEPTIONS,
	/** Number of Functions */
	FUNCTIONS,
	/**
	 * Lines of code, calculated with the function strings (and not the whole
	 * source file).
	 */
	LOC;
}