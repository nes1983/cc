package ch.unibe.scg.cells;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Qualifier;

import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Providers;
import com.google.protobuf.ByteString;

/** Check {@link LocalCounter}. */
public final class LocalCounterTest {

	/** A codec for integers. */
	public static class IntegerCodec implements Codec<Integer> {
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

	/** A counter for io exceptions. */
	@Qualifier
	@Target({ FIELD, PARAMETER, METHOD })
	@Retention(RUNTIME)
	public static @interface IOExceptions {}

	/** A counter for user exceptions. */
	@Qualifier
	@Target({ FIELD, PARAMETER, METHOD })
	@Retention(RUNTIME)
	public static @interface UsrExceptions {}

	/** A simple mapper, that maps integers exactly to themselves. */
	public static class IdentityMapper implements Mapper<Integer, Integer> {
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
			for (Integer i : row) {
				sink.write(i);
				ioCounter.increment(1L);
				usrCounter.increment(2L);
			}
		}
	}

	/** Checks that counter stays alive after being serialized. */
	@Test
	public void testCounterSerializationIsLive() throws IOException {
		Set<LocalCounter> registry = new LinkedHashSet<>();
		LocalCounter  cnt = new LocalCounter("cnt1", Providers.of(new AtomicLong()),
				Providers.of(registry));
		LocalCounter cntCopy = ShallowSerializingCopy.clone(cnt);
		cntCopy.increment(1L);

		assertThat(cntCopy.toString(), equalTo("cnt1: 1"));
		assertThat(cnt.toString(), equalTo("cnt1: 1"));
		assertThat(registry.toString(), equalTo("[cnt1: 1]"));
	}

	/** Checks that counters do not carry their values between pipeline stages. */
	@Test
	public void testCounterResetsAcrossStages() throws IOException, InterruptedException {
		Module m = makeCellsModule();

		Injector inj = Guice.createInjector(m, new LocalExecutionModule());

		try (InMemoryPipeline<Integer, Integer> pipe
				= inj.getInstance(InMemoryPipeline.Builder.class)
					.make(Cells.shard(Cells.encode(generateSequence(1000), new IntegerCodec())))) {

			IdentityMapper mapper = inj.getInstance(IdentityMapper.class);
			run(pipe, mapper);

			assertThat(mapper.finalIoCount, equalTo("ch.unibe.scg.cells.LocalCounterTest$IOExceptions: 1000"));
			assertThat(mapper.finalUsrCount, equalTo("ch.unibe.scg.cells.LocalCounterTest$UsrExceptions: 2000"));
		}
	}

	/** Checks counters being printed as pipeline progresses. */
	@Test
	public void testCounterProgressIsPrinted() throws IOException, InterruptedException {
		Module m = makeCellsModule();

		Injector inj = Guice.createInjector(m, new LocalExecutionModule());
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		PrintStream out = new PrintStream(bos, true); // flush after each write.

		try (InMemoryPipeline<Integer, Integer> pipe
				= new InMemoryPipeline<>(
						Cells.shard(Cells.encode(generateSequence(1000), new IntegerCodec())),
						inj.getInstance(PipelineStageScope.class),
						inj.getInstance(Key.get(new TypeLiteral<Provider<Set<LocalCounter>>>(){})),
						out)) {

			IdentityMapper mapper = inj.getInstance(IdentityMapper.class);
			run(pipe, mapper);

			String log = bos.toString(Charsets.UTF_8.toString());

			// each line in the log should be of following format: <some name>: <number>.
			Pattern linePattern = Pattern.compile("\\S+: \\d+");
			for (String logLine: log.split(System.lineSeparator())) {
				assertThat(linePattern.matcher(logLine).matches(), equalTo(true));
			}

			// these final lines should occur once per pipeline stage. In the test we have 2 stages.
			assertThat(countMatches(log, "ch.unibe.scg.cells.LocalCounterTest$IOExceptions: 1000"), equalTo(2));
			assertThat(countMatches(log, "ch.unibe.scg.cells.LocalCounterTest$UsrExceptions: 2000"), equalTo(2));

			// TODO: this test will print 0 counters for large total number. The implementation should be fixed.
		}
	}

	/** Runs a pipeline that maps ints to ints. */
	public static void run(Pipeline<Integer, Integer> pipe,
			Mapper<Integer, Integer> mapper) throws IOException, InterruptedException {
		pipe.influx(new IntegerCodec())
			.map(mapper)
			.shuffle(new IntegerCodec())
			.mapAndEfflux(mapper, new IntegerCodec());
	}

	/** Generates a sequence of integers of specified length. */
	public static Iterable<Integer> generateSequence(int number) {
		List<Integer> ret = new ArrayList<>(number);

		for (int i = 0; i < number; i++) {
			ret.add(i);
		}
		return ret;
	}

	private static int countMatches(String str, String sub) {
		int lastIndex = 0;
		int count = 0;

		while (lastIndex != -1) {
			lastIndex = str.indexOf(sub, lastIndex);
			if (lastIndex != -1) {
				count++;
				lastIndex += sub.length();
			}
		}

		return count;
	}

	private static Module makeCellsModule() {
		return new CellsModule() {
			@Override protected void configure() {
				installCounter(IOExceptions.class, new LocalCounterModule());
				installCounter(UsrExceptions.class, new LocalCounterModule());
			}
		};
	}
}