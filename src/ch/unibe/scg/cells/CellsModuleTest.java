package ch.unibe.scg.cells;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.inject.Inject;
import javax.inject.Qualifier;

import org.junit.Test;

import ch.unibe.scg.cells.hadoop.HadoopPipelineTest.Eff;
import ch.unibe.scg.cells.hadoop.HadoopPipelineTest.In;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;

/** Check {@link CellsModule}. */
public final class CellsModuleTest {
	/** Simple smoke test that checks if injection of tables falls apart by itself. */
	@Test
	public void testTableInjection() {
		final TableModule nullTableModule = new TableModule() {
			@Override public void configure(Binder binder) {
				// Do nothing.
			}
		};

		Injector i = Guice.createInjector(new CellsModule() {
			@Override
			protected void configure() {
				installTable(
						In.class,
						new TypeLiteral<Integer>() {},
						IntCodec.class,
						new InMemoryStorage(),
						nullTableModule);
				installTable(
						Eff.class,
						new TypeLiteral<Integer>() {},
						IntCodec.class,
						new InMemoryStorage(),
						nullTableModule);
			}
		});

		i.getInstance(InjectMe.class);
		// TODO: Check if the right table name was injected.
	}

	/** Checks, if counter injection works at all **/
	@Test
	public void testCounterInjection() {
		Injector i = Guice.createInjector(new CellsModule() {
			@Override protected void configure() {
				installCounter(IOExceptions.class, new LocalCounterModule());
			}
		});

		Counter counter = i.getInstance(InjectedCounterHolder.class).counter;
		counter.increment(42L);

		assertThat(counter, instanceOf(LocalCounter.class));
		assertThat(counter.toString(), equalTo("ch.unibe.scg.cells.CellsModuleTest$IOExceptions: 42"));
	}

	@Qualifier
	@Target({ FIELD, PARAMETER, METHOD })
	@Retention(RUNTIME)
	private static @interface IOExceptions {}

	private static class InjectedCounterHolder {
		final Counter counter;

		@Inject
		InjectedCounterHolder(@IOExceptions Counter counter) {
			this.counter = counter;
		}
	}

	private static class InjectMe {
		@SuppressWarnings("unused") // We just check if Guice could inject at all.
		@Inject
		InjectMe(@In Source<Integer> srcBla, @Eff Source<Integer> srcBli) {}
	}

	private static class IntCodec implements Codec<Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<Integer> encode(Integer s) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Integer decode(Cell<Integer> encoded) throws IOException {
			throw new UnsupportedOperationException();
		}
	}
}