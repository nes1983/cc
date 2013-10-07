package ch.unibe.scg.cells;

import java.io.IOException;

import javax.inject.Inject;

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