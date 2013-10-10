package ch.unibe.scg.cells;

/** Represents a local {@link Counter}, that can be used inside {@link InMemoryPipeline} **/
class LocalCounter implements Counter {
	private static final long serialVersionUID = 1L;

	@Override
	public void increment(long cnt) {
		throw new RuntimeException("Not implemented yet"); // TODO: implement
	}
}