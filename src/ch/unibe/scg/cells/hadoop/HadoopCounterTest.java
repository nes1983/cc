package ch.unibe.scg.cells.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import ch.unibe.scg.cells.CellsModule;
import ch.unibe.scg.cells.LocalCounterTest;
import ch.unibe.scg.cells.LocalCounterTest.IOExceptions;
import ch.unibe.scg.cells.LocalCounterTest.IntegerCodec;
import ch.unibe.scg.cells.LocalCounterTest.UsrExceptions;
import ch.unibe.scg.cells.Sink;
import ch.unibe.scg.cells.hadoop.HadoopPipelineTest.In;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.protobuf.ByteString;

@SuppressWarnings("javadoc")
public final class HadoopCounterTest {
	final private ByteString FAMILY = ByteString.copyFromUtf8("f");

	/**Checks that counters do not carry their values between pipeline stages. */
	@Test
	public void testCounterResetsAcrossStages() throws IOException, InterruptedException {
		TableAdmin tableAdmin = Guice.createInjector(new UnibeModule()).getInstance(TableAdmin.class);

		try (Table<Integer> in = tableAdmin.createTemporaryTable(FAMILY);
				Table<Integer> eff = tableAdmin.createTemporaryTable(FAMILY)) {
			Module m = new CellsModule() {
				@Override protected void configure() {
					installCounter(IOExceptions.class, new HadoopCounterModule());
					installCounter(UsrExceptions.class, new HadoopCounterModule());
					installTable(
							In.class,
							new TypeLiteral<Integer>() {},
							IntegerCodec.class,
							new HBaseStorage(), new HBaseTableModule<>(in));
				}
			};

			Injector inj = Guice.createInjector(m,new UnibeModule());
			try (Sink<Integer> s = inj.getInstance(Key.get(new TypeLiteral<Sink<Integer>>() {}, In.class))) {
				for (Integer i : LocalCounterTest.generateSequence(1000)) {
					s.write(i);
				}
			}

			HadoopPipeline<Integer, Integer> pipe
				= HadoopPipeline.fromTableToTable(inj.getInstance(Configuration.class), in, eff);

			inj.getInstance(LocalCounterTest.Runner.class).run(pipe);
			// TODO: add assertions.
		}
	}
}
