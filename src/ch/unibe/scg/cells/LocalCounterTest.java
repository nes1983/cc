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
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;
import javax.inject.Qualifier;

import org.junit.Test;

import com.google.common.primitives.Ints;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Providers;
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

	@Qualifier
	@Target({ FIELD, PARAMETER, METHOD })
	@Retention(RUNTIME)
	private static @interface UsrExceptions {}

	private static class IdentityMapper implements Mapper<Integer, Integer> {
		private static final long serialVersionUID = 1L;

		final Counter ioCounter;
		final Counter usrCounter;

		String finalIoCount;
		String finalUsrCount;

		@Inject
		IdentityMapper(@IOExceptions Counter ioCounter, @UsrExceptions Counter usrCounter) {
			this.ioCounter = ioCounter;
			this.usrCounter = usrCounter;
		}

		@Override
		public void close() {
			finalIoCount = ioCounter.toString();
			finalUsrCount = usrCounter.toString();
		}

		@Override
		public void map(Integer first, OneShotIterable<Integer> row, Sink<Integer> sink) throws IOException,
				InterruptedException {
			for(Integer i : row) {
				sink.write(i);
				ioCounter.increment(1L);
				usrCounter.increment(2L);
			}
		}
	}

	/**Checks that counter stays alive after being serialized */
	@Test
	public void testCounterSerializationIsLive() throws IOException {
		LocalCounter  cnt = new LocalCounter("cnt1", Providers.of(new AtomicLong()));
		LocalCounter cntCopy = ShallowSerializingCopy.clone(cnt);
		cntCopy.increment(1L);

		assertThat(cntCopy.toString(), equalTo("cnt1: 1"));
		assertThat(cnt.toString(), equalTo("cnt1: 1"));
	}

	/**Checks that counters do not carry their values between pipeline stages*/
	@Test
	public void testCounterResetsAcrossStages() throws IOException, InterruptedException {
		Module m = new CellsModule() {
			@Override protected void configure() {
				installCounter(IOExceptions.class, new LocalCounterModule());
				installCounter(UsrExceptions.class, new LocalCounterModule());
			}
		};

		Injector inj = Guice.createInjector(m, new LocalExecutionModule());

		try(InMemoryShuffler<Integer> eff = InMemoryShuffler.getInstance()) {
			InMemoryPipeline<Integer, Integer> pipe
					= InMemoryPipeline.make(InMemoryShuffler.copyFrom(generateSequence(1000), new IntegerCodec()), eff);
			// TODO: Fix next line.
			inj.injectMembers(pipe);
			IdentityMapper mapper = inj.getInstance(IdentityMapper.class);
			pipe.influx(new IntegerCodec())
					.map(mapper)
					.shuffle(new IntegerCodec())
					.mapAndEfflux(mapper, new IntegerCodec());

			assertThat(mapper.finalIoCount, equalTo("ch.unibe.scg.cells.LocalCounterTest$IOExceptions: 1000"));
			assertThat(mapper.finalUsrCount, equalTo("ch.unibe.scg.cells.LocalCounterTest$UsrExceptions: 2000"));
		}
	}

	private static Iterable<Integer> generateSequence(int number) {
		List<Integer> ret = new ArrayList<>(number);

		for(int i = 0; i < number; i++) {
		    ret.add(i);
		}
		return ret;
	}
}