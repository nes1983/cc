package ch.unibe.scg.cells.hadoop;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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

	/**
	 * A proxy object for SequenceFileAsBinaryInputFormat,
	 * but using ImmutableBytesWritable in the generic type.
	 */
	private static class RawTextFileFormat
			extends FileInputFormat<ImmutableBytesWritable, ImmutableBytesWritable> {
		final private TextInputFormat underlying = new TextInputFormat();

		@Override
		public RecordReader<ImmutableBytesWritable, ImmutableBytesWritable>
				createRecordReader(InputSplit split, TaskAttemptContext context)
						throws IOException, InterruptedException {
			return new RecordReaderProxy(underlying.createRecordReader(split, context));
		}
	}

	private static class RecordReaderProxy
			extends RecordReader<ImmutableBytesWritable, ImmutableBytesWritable> {
		final private RecordReader<LongWritable, Text> underlying;

		RecordReaderProxy(RecordReader<LongWritable, Text> recordReader) {
			this.underlying = recordReader;
		}

		@Override
		public void close() throws IOException {
			underlying.close();
		}

		@Override
		public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
			return new ImmutableBytesWritable(Longs.toByteArray(underlying.getCurrentKey().get()));
		}

		@Override
		public ImmutableBytesWritable getCurrentValue() throws IOException, InterruptedException {
			return new ImmutableBytesWritable(underlying.getCurrentValue().getBytes());
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return underlying.getProgress();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext ctx) throws IOException, InterruptedException {
			underlying.initialize(split, ctx);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return underlying.nextKeyValue();
		}
	}

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
			HadoopPipeline<File, File> pipe = HadoopPipeline.fromHadoopToTable(
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
