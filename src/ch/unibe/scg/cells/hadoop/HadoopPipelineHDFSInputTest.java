package ch.unibe.scg.cells.hadoop;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;

@SuppressWarnings("javadoc")
public final class HadoopPipelineHDFSInputTest {
	/**
	 * A proxy object for SequenceFileAsBinaryInputFormat,
	 * but using ImmutableBytesWritable in the generic type.
	 */
	static class SequenceFileInputFormat
			extends FileInputFormat<ImmutableBytesWritable, ImmutableBytesWritable> {
		final private SequenceFileAsBinaryInputFormat underlying
				= new SequenceFileAsBinaryInputFormat();

		@Override
		public RecordReader<ImmutableBytesWritable, ImmutableBytesWritable>
				createRecordReader(InputSplit split, TaskAttemptContext context)
						throws IOException, InterruptedException {
			return new RecordReaderProxy(underlying.createRecordReader(split, context));
		}
	}

	private static class RecordReaderProxy
			extends RecordReader<ImmutableBytesWritable, ImmutableBytesWritable> {
		final private RecordReader<BytesWritable, BytesWritable> underlying;

		RecordReaderProxy(RecordReader<BytesWritable, BytesWritable> underlying) {
			this.underlying = underlying;
		}

		@Override
		public void close() throws IOException {
			underlying.close();
		}

		@Override
		public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
			return new ImmutableBytesWritable(underlying.getCurrentKey().getBytes());
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

	// TODO: Ddd test method.
}
