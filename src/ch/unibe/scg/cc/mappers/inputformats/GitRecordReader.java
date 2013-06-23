package ch.unibe.scg.cc.mappers.inputformats;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/** Instances are not threadsafe */
class GitRecordReader extends RecordReader<Text, BytesWritable> {
	private static final int BUFFER_SIZE = 8192;
	static Logger logger = Logger.getLogger(GitRecordReader.class.getName());
	private static final String REGEX_PACKFILE = "(.+)objects/pack/pack-[a-f0-9]{40}\\.pack";
	private FSDataInputStream fsin;
	private Text currentKey;
	private BytesWritable currentValue;
	private boolean isFinished = false;
	private Queue<Path> packFilePaths;
	private FileSystem fs;

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException,
			InterruptedException {
		FileSplit split = (FileSplit) inputSplit;
		Configuration conf = taskAttemptContext.getConfiguration();
		Path path = split.getPath();
		fs = path.getFileSystem(conf);

		packFilePaths = new ArrayDeque<>();
		logger.finer("yyy start finding pack files " + path);
		findPackFilePaths(fs, path, packFilePaths);

		if (packFilePaths.isEmpty()) {
			isFinished = true;
			return;
		}
	}

	private void findPackFilePaths(FileSystem fs, Path path, Queue<Path> listToFill) throws IOException {
		FileStatus[] fstatus = fs.listStatus(path);
		for (FileStatus f : fstatus) {
			Path p = f.getPath();
			logger.finer("yyy scanning: " + f.getPath() + " || " + f.getPath().getName());
			if (f.isFile() && f.getPath().toString().matches(REGEX_PACKFILE)) {
				listToFill.add(p);
			} else if (f.isDirectory()) {
				findPackFilePaths(fs, p, listToFill);
			}
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (packFilePaths.isEmpty()) {
			isFinished = true;
			logger.finer("yyy packFilePaths is empty... finished");
			return false;
		}

		Path packFilePath = packFilePaths.poll();
		logger.finer("yyy opening " + packFilePath);

		fsin = fs.open(packFilePath);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		byte[] temp = new byte[BUFFER_SIZE];
		while (true) {
			int bytesRead = fsin.read(temp, 0, BUFFER_SIZE);
			if (bytesRead > 0) {
				bos.write(temp, 0, bytesRead);
			} else {
				break;
			}
		}
		currentValue = new BytesWritable(bos.toByteArray());
		currentKey = new Text(packFilePath.toString());

		return true;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (isFinished) {
			return 1;
		}
		return 0;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return currentKey;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
	}

	@Override
	public void close() throws IOException {
		if (fsin != null) {
			fsin.close();
		}
	}
}
