package ch.unibe.scg.cells;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Qualifier;

import org.junit.Test;

import com.google.common.primitives.Ints;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.protobuf.ByteString;

/** Check {@link LocalCounter}. */
public final class LocalCounterTest {
	private static class IntegerCodec implements Codec<Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<Integer> encode(Integer i) {
			return Cell.make(ByteString.copyFrom(Ints.toByteArray(i)), ByteString.copyFromUtf8("t"),
					ByteString.copyFromUtf8("t"));
		}

		@Override
		public Integer decode(Cell<Integer> encoded) throws IOException {
			return new Integer(Ints.fromByteArray(encoded.getRowKey().toByteArray()));
		}
	}

	@Qualifier
	@Target({ FIELD, PARAMETER, METHOD })
	@Retention(RUNTIME)
	private static @interface IOExceptions {}

	private static class IdentityMapper implements Mapper<Integer, Integer> {
		private static final long serialVersionUID = 1L;

		final Counter counter;

		@Inject
		IdentityMapper(@IOExceptions Counter counter) {
			this.counter = counter;
		}

		@Override
		public void close() { }

		@Override
		public void map(Integer first, OneShotIterable<Integer> row, Sink<Integer> sink) throws IOException,
				InterruptedException {
			for(Integer i : row) {
				sink.write(i);
				counter.increment(1L);
			}
		}
	}

	/**Checks that counter stays alive after being serialized */
	@Test
	public void testCounterSerializationIsLive() throws IOException {
		LocalCounter  cnt = new LocalCounter("cnt1");
		LocalCounter cntCopy = ShallowSerializingCopy.clone(cnt);
		cntCopy.increment(1L);

		assertThat(cntCopy.toString(), equalTo("cnt1: 1"));
		assertThat(cnt.toString(), equalTo("cnt1: 1"));
	}

	/**Checks that counters do not carry their values between pipeline stages*/
	@Test
	public void testCounterResetsAcrossStages() throws IOException, InterruptedException {
		Module tab = new CellsModule() {
			@Override protected void configure() {
				installCounter(IOExceptions.class, new LocalCounterModule());
			}
		};

		Injector inj = Guice.createInjector(tab);

		try(InMemoryShuffler<Integer> eff = InMemoryShuffler.getInstance()) {
			InMemoryPipeline<Integer, Integer> pipe
					= InMemoryPipeline.make(InMemoryShuffler.copyFrom(generateSequence(1000), new IntegerCodec()), eff);
			IdentityMapper mapper = inj.getInstance(IdentityMapper.class);
			run(pipe, mapper);
			// TODO: note, that stage-aware counter gives 0 here. 2000 is the supposed result at the end of two stages
			assertThat(mapper.counter.toString(), equalTo("ch.unibe.scg.cells.LocalCounterTest$IOExceptions: 2000"));
		}
	}

	void run(Pipeline<Integer, Integer> pipeline, Mapper<Integer, Integer> what) throws IOException, InterruptedException {
		pipeline.influx(new IntegerCodec())
			.map(what)
			.shuffle(new IntegerCodec())
			.mapAndEfflux(what, new IntegerCodec());
	}

	private static Iterable<Integer> generateSequence(int number) {
		List<Integer> ret = new ArrayList<>(number);

		for(int i = 0; i < number; i++) {
		    ret.add(i);
		}
		return ret;
	}
}