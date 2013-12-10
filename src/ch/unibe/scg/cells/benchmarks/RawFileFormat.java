package ch.unibe.scg.cells.benchmarks;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;

class RawFileFormat extends FileInputFormat<ImmutableBytesWritable, ImmutableBytesWritable> {
	@Override
	public RecordReader<ImmutableBytesWritable, ImmutableBytesWritable> createRecordReader(
			InputSplit is, TaskAttemptContext c) throws IOException, InterruptedException {
		return new RawFileRecordReader();
	}

	/** Input format for a directory of files. No recursion. */
	static class RawFileRecordReader extends
			RecordReader<ImmutableBytesWritable, ImmutableBytesWritable> {
		private ImmutableBytesWritable currentKey;
		private ImmutableBytesWritable currentValue;
		private boolean isFinished;
		private FileSplit split;
		private FileSystem fs;

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
				throws IOException, InterruptedException {
			split = (FileSplit) inputSplit; // Cast will always hold for FileInputFormats.
											// Taken from SequenceFileRecordReader#initialize.
			fs = split.getPath().getFileSystem(taskAttemptContext.getConfiguration());
											// Again, this line is stolen from SequenceFileRecordReader.
		}

		@Override
		public void close() throws IOException {
			if (fs != null) {
				fs.close();
				fs = null;
			}
		}

		@Override
		public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
			return currentKey;
		}

		@Override
		public ImmutableBytesWritable getCurrentValue() throws IOException, InterruptedException {
			return currentValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			if (isFinished) {
				return 1.0f;
			}

			return 0.0f;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (isFinished) {
				return false;
			}
			isFinished = true;

			FileStatus stat = fs.getFileStatus(split.getPath());
			if (stat.isDirectory()) {
				throw new IOException("This input format is for a directory of files. No recursion.");
			}

			currentKey = new ImmutableBytesWritable(split.getPath().toString().getBytes(Charsets.UTF_8));

			try (FSDataInputStream fsin = fs.open(split.getPath())) {
				currentValue = new ImmutableBytesWritable(ByteStreams.toByteArray(fsin));
			}

			return true;
		}
	}
}