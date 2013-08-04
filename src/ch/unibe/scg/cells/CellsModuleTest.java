package ch.unibe.scg.cells;

import java.io.IOException;

import javax.inject.Inject;

import org.junit.Test;

import ch.unibe.scg.cells.Annotations.FamilyName;
import ch.unibe.scg.cells.Annotations.TableName;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.protobuf.ByteString;

/** Check {@link CellsModule}. */
public final class CellsModuleTest {
	/** Simple smoke test that checks if injection of tables falls apart by itself. */
	@Test
	public void testTableInjection() {
		Injector i = Guice.createInjector(new CellsModule() {
			@Override
			protected void configure() {
				installTable("bla",
						ByteString.EMPTY,
						TableName.class,
						new TypeLiteral<Integer>() {},
						IntCodec.class,
						new InMemoryStorage());
				installTable("blub",
						ByteString.EMPTY,
						FamilyName.class,
						new TypeLiteral<Integer>() {},
						IntCodec.class,
						new InMemoryStorage());
			}
		});

		i.getInstance(InjectMe.class);
		// TODO: Check if the right table name was injected.
	}

	private static class InjectMe {
		@SuppressWarnings("unused") // Unavoidable. We're checking if the dependencies can be injected.
		@Inject
		InjectMe(@TableName Source<Integer> srcBla, @FamilyName Source<Integer> srcBli) {}
	}

	private static class IntCodec implements Codec<Integer> {
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