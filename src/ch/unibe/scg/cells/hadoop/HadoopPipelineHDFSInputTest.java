package ch.unibe.scg.cells.hadoop;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.Cells;
import ch.unibe.scg.cells.Codec;
import ch.unibe.scg.cells.Mapper;
import ch.unibe.scg.cells.OneShotIterable;
import ch.unibe.scg.cells.Sink;
import ch.unibe.scg.cells.Source;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.ByteString;

@SuppressWarnings("javadoc")
public final class HadoopPipelineHDFSInputTest {
	final private static ByteString family = ByteString.copyFromUtf8("f");

	private static class IdentityMapper implements Mapper<File, File> {
		private static final long serialVersionUID = 1L;

		@Override
		public void close() throws IOException { }

		@Override
		public void map(File first, OneShotIterable<File> row, Sink<File> sink) throws IOException,
				InterruptedException {
			for (File f : row) {
				sink.write(f);
			}
		}
	}

	private static class File {
		final long lineNumber;
		final String contents;

		File(long lineNumber, String contents) {
			this.lineNumber = lineNumber;
			this.contents = contents;
		}
	}

	private static class FileCodec implements Codec<File> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<File> encode(File f) {
			return Cell.make(ByteString.copyFrom(Longs.toByteArray(f.lineNumber)),
					ByteString.copyFromUtf8("1"),
					ByteString.copyFromUtf8(f.contents));
		}

		@Override
		public File decode(Cell<File> encoded) throws IOException {
			return new File(Longs.fromByteArray(encoded.getRowKey().toByteArray()),
					encoded.getCellContents().toStringUtf8());
		}
	}

	@Test
	public void testReadBigSCript() throws IOException, InterruptedException {
		Injector injector = Guice.createInjector(new UnibeModule());

		int cnt = 0;
		try (Table<File> tab = injector.getInstance(TableAdmin.class).createTemporaryTable(family)) {
			HadoopPipeline<File, File> pipe = HadoopPipeline.fromHDFSToTable(
					injector.getInstance(Configuration.class),
					RawTextFileFormat.class,
					new Path("hdfs://haddock.unibe.ch/user/nes/upgrade-squeezestep.script"),
					tab);
			pipe
				.influx(new FileCodec())
				.mapAndEfflux(new IdentityMapper(), new FileCodec());

			try (Source<File> files = Cells.decodeSource(tab.asCellSource(), new FileCodec())) {
				for (Iterable<File> row : files) {
					cnt += Iterables.size(row);
				}
			}
		}

		assertThat(cnt, is(7836)); // TODO: wc reports the file size as 4798. Why the difference?
	}
}
