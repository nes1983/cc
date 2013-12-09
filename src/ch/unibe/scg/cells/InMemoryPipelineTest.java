package ch.unibe.scg.cells;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.ByteString;

@SuppressWarnings("javadoc")
public final class InMemoryPipelineTest {
	private static class IdentityMapper implements Mapper<Cell<Void>, Cell<Void>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void close() throws IOException { }

		@Override
		public void map(Cell<Void> first, OneShotIterable<Cell<Void>> row, Sink<Cell<Void>> sink) throws IOException,
				InterruptedException {
			for (Cell<Void> c : row) {
				sink.write(c);
			}
		}
	}

	private static class IdentityCodec implements Codec<Cell<Void>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<Cell<Void>> encode(Cell<Void> obj) {
			return Cell.make(obj.getRowKey(), obj.getColumnKey(), obj.getCellContents());
		}

		@Override
		public Cell<Void> decode(Cell<Cell<Void>> cell) throws IOException {
			return Cell.make(cell.getRowKey(), cell.getColumnKey(), cell.getCellContents());
		}
	}

	@Test
	public void testIdentityMapper() throws IOException, InterruptedException {
		Injector inj = Guice.createInjector(new LocalExecutionModule());
		List<Cell<Cell<Void>>> src = Arrays.asList(
				Cell.<Cell<Void>> make(ByteString.copyFromUtf8("r1"), ByteString.copyFromUtf8("c1"), ByteString.EMPTY),
				Cell.<Cell<Void>> make(ByteString.copyFromUtf8("r2"), ByteString.copyFromUtf8("c1"), ByteString.EMPTY),
				Cell.<Cell<Void>> make(ByteString.copyFromUtf8("r2"), ByteString.copyFromUtf8("c2"), ByteString.EMPTY),
				Cell.<Cell<Void>> make(ByteString.copyFromUtf8("r3"), ByteString.copyFromUtf8("c1"), ByteString.EMPTY),
				Cell.<Cell<Void>> make(ByteString.copyFromUtf8("r4"), ByteString.copyFromUtf8("c1"), ByteString.EMPTY),
				Cell.<Cell<Void>> make(ByteString.copyFromUtf8("r4"), ByteString.copyFromUtf8("c3"), ByteString.EMPTY));

		try (InMemoryPipeline<Cell<Void>, Cell<Void>> pipe
				= inj.getInstance(InMemoryPipeline.Builder.class).<Cell<Void>, Cell<Void>> make(Cells.shard(src))) {
			pipe
				.influx(new IdentityCodec())
				.mapAndEfflux(new IdentityMapper(), new IdentityCodec());

			try (Source<Cell<Void>> rows = pipe.lastEfflux()) {
				List<Cell<Void>> actual = new ArrayList<>();
				for (Iterable<Cell<Void>> row : rows) {
					for (Cell<Void> c : row) {
						actual.add(c);
					}
				}

				// Identity mapping should produce the same output as was input.
				assertEquals(src, actual);

				// Check the grouping.
				Assert.assertEquals(Lists.newArrayList(Iterables.get(rows, 0)),
						Arrays.asList(src.get(0)));
				Assert.assertEquals(Lists.newArrayList(Iterables.get(rows, 1)),
						Arrays.asList(src.get(1), src.get(2)));
				Assert.assertEquals(Lists.newArrayList(Iterables.get(rows, 2)),
						Arrays.asList(src.get(3)));
				Assert.assertEquals(Lists.newArrayList(Iterables.get(rows, 3)),
						Arrays.asList(src.get(4), src.get(5)));
			}
		}
	}
}
