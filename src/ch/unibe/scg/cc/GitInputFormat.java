package ch.unibe.scg.cc;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.eclipse.jgit.lib.Constants;

import ch.unibe.scg.cc.Protos.GitRepo;
import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.Codec;

import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;

/**
 * File input format for bare git repositories. The input path must be the a
 * packfile in a git repo. The only two files that are looked at is the packfile
 * and the pack refs.
 */
class GitInputFormat extends FileInputFormat<ImmutableBytesWritable, ImmutableBytesWritable> implements
		Serializable {
	final private static long serialVersionUID = 1L;

	/** Codec that reads the input format into a GitRepo compatible with GitInputFormat. */
	static final class GitRepoCodec implements Codec<GitRepo> {
		final private static long serialVersionUID = 1L;
		final private static byte[] COL_KEY = ByteString.copyFromUtf8("project").toByteArray();

		@Override
		public Cell<GitRepo> encode(GitRepo r) {
			return Cell.make(ByteString.copyFromUtf8(r.getProjectName()), ByteString.copyFrom(COL_KEY),
					r.toByteString());
		}

		@Override
		public GitRepo decode(Cell<GitRepo> encoded) throws IOException {
			return GitRepo.parseFrom(encoded.getCellContents());
		}
	}

	private static class GitPathRecordReader
			extends RecordReader<ImmutableBytesWritable, ImmutableBytesWritable> {
		private static final long MAX_PACKFILE_SIZE_MB = 500;
		private ImmutableBytesWritable currentKey;
		private ImmutableBytesWritable currentValue;
		private boolean isFinished;
		private FileSplit split;
		private FileSystem fs;

		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
				throws IOException,	InterruptedException {
			split = (FileSplit) inputSplit; // Cast will always hold for FileInputFormats.
											// Taken from SequenceFileRecordReader#initialize.
			fs = split.getPath().getFileSystem(taskAttemptContext.getConfiguration());
											// Again, this line is stolen from SequenceFileRecordReader.
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (isFinished) {
				return false;
			}

			// TODO: Delete this crutch.
			// It's here because there's an irrelevant file in the input …
			if (split.getPath().getName().equals("index")) {
				return false;
			}

			// Find packFilePath.
			FileStatus[] potentialPackFiles = fs.listStatus(new Path(split.getPath(), "objects/pack/"));

			Path packFilePath = null;
			for (FileStatus f : potentialPackFiles) {
				if (f.getPath().getName().endsWith(".pack")) {
					// Found pack file.
					if (packFilePath != null) {
						throw new IOException("We accept only fully packed git repos as "
								+ "input. But there were two pack files.");
					}
					packFilePath = f.getPath();
				}
			}
			if (packFilePath == null) {
				throw new IOException("There was no pack file in the repo!");
			}
			
			// Check pack file size because with protobuf 2.4.0a the pack file gets copied 3 times in memory.
			if (fs.getStatus(packFilePath).getUsed() / 1048576l > MAX_PACKFILE_SIZE_MB) {
				throw new IOException("Pack file exceeded size limit of " + MAX_PACKFILE_SIZE_MB + " MB.");
			}

			byte[] packFile;
			try (FSDataInputStream fsin = fs.open(packFilePath)) {
				packFile = ByteStreams.toByteArray(fsin);
			}

			Path packRefsPath = new Path(split.getPath(), Constants.PACKED_REFS);
			byte[] packRefs;
			try (FSDataInputStream fsin = fs.open(packRefsPath)) {
				packRefs = ByteStreams.toByteArray(fsin);
			}

			Cell<GitRepo> cell = new GitRepoCodec().encode(
					GitRepo.newBuilder()
						.setProjectName(packFilePath.toString())
						.setPackFile(ByteString.copyFrom(packFile))
						.setPackRefs(ByteString.copyFrom(packRefs)).build());

			currentKey = new ImmutableBytesWritable(cell.getRowKey().toByteArray());
			currentValue = new ImmutableBytesWritable(cell.getCellContents().toByteArray());

			isFinished = true;
			return true;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			if (isFinished) {
				return 1.0f;
			}

			return 0.0f;
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
		public void close() throws IOException {
			if (fs != null) {
				fs.close();
			}
		}
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	@Override
	public RecordReader<ImmutableBytesWritable, ImmutableBytesWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new GitPathRecordReader();
	}
}