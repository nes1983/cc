package ch.unibe.scg.cells.hadoop;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.google.common.primitives.Longs;


/** A FileInputFormat that allows to read files in hdfs. Encodes files as sequence of immutable bytes. */
public class RawTextFileFormat extends FileInputFormat<ImmutableBytesWritable, ImmutableBytesWritable> {
	final private TextInputFormat underlying = new TextInputFormat();

	private RawTextFileFormat() {
		// Don't subclass
	}

	private static class RecordReaderProxy extends RecordReader<ImmutableBytesWritable, ImmutableBytesWritable> {
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

	@Override
	public RecordReader<ImmutableBytesWritable, ImmutableBytesWritable>	createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new RecordReaderProxy(underlying.createRecordReader(split, context));
	}
}
