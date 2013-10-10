package ch.unibe.scg.cells.hadoop;

import ch.unibe.scg.cells.Counter;

/** Represents a {@link Counter}, that uses a hadoop counter service to display information to user. **/
class HadoopCounter implements Counter {
	private static final long serialVersionUID = 1L;

	@Override
	public void increment(long cnt) {
		throw new RuntimeException("Not implemented yet"); // TODO: implement
	}
}