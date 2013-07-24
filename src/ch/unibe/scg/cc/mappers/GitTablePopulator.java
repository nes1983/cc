package ch.unibe.scg.cc.mappers;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Named;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import ch.unibe.scg.cc.Annotations.Java;
import ch.unibe.scg.cc.GitPopulator;
import ch.unibe.scg.cc.Populator;
import ch.unibe.scg.cc.WrappedRuntimeException;
import ch.unibe.scg.cc.mappers.inputformats.GitPathInputFormat;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.inject.Inject;

/** Load in Har file of git repositories and populate our tables. */
public class GitTablePopulator implements Runnable {
	static Logger logger = Logger.getLogger(GitTablePopulator.class.getName());
	private static final String CORE_SITE_PATH = "/etc/hadoop/conf/core-site.xml";
	private static final String MAP_MEMORY = "4000";
	private static final String MAPRED_CHILD_JAVA_OPTS = "-Xmx3300m";
	// num_projects: projects.har 405 | testdata.har: 2 | dataset.har 2246
	/** needs to correspond with the path defined in DataFetchPipeline.sh */
	private static final String PROJECTS_HAR_PATH = "har://hdfs-haddock.unibe.ch/projects/dataset.har";
	private final MapReduceLauncher launcher;

	@Inject
	GitTablePopulator(MapReduceLauncher launcher) {
		this.launcher = launcher;
	}

	@Override
	public void run() {
		try {
			Configuration config = new Configuration();
			config.set(MRJobConfig.NUM_REDUCES, "0");
			// we don't want multiple mappers on the same input folder as we
			// directly write to HBase in our map task, hence speculative
			// execution is disabled
			config.set(MRJobConfig.MAP_SPECULATIVE, "false");
			// set to 1 if unsure TODO: check max mem allocation if only 1 jvm
			config.set(MRJobConfig.JVM_NUMTASKS_TORUN, "-1");
			config.set(MRJobConfig.MAP_MAX_ATTEMPTS, "1");
			config.set(MRJobConfig.TASK_TIMEOUT, "432000000"); // 5 days
			config.set(MRJobConfig.MAP_MEMORY_MB, MAP_MEMORY);
			config.set(MRJobConfig.MAP_JAVA_OPTS, MAPRED_CHILD_JAVA_OPTS);
			// don't abort the whole job if a pack file is corrupt:
			config.set(MRJobConfig.MAP_FAILURES_MAX_PERCENT, "99");
			config.setClass(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, GitPathInputFormat.class, InputFormat.class);
			config.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, NullOutputFormat.class, OutputFormat.class);
			String inputPaths = getInputPaths();
			config.set(FileInputFormat.INPUT_DIR, inputPaths);
			config.set(Constants.GUICE_CUSTOM_MODULES_ANNOTATION_STRING, HBaseModule.class.getName());

			logger.finer("Input paths: " + inputPaths);
			launcher.launchMapReduceJob("gitPopulate", config, Optional.<String> absent(), Optional.<String> absent(),
					null, GitTablePopulatorMapper.class.getName(), Optional.<String> absent(), Text.class,
					IntWritable.class);
		} catch (IOException | ClassNotFoundException e) {
			throw new WrappedRuntimeException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return; // Exit.
		}
	}

	private String getInputPaths() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path(CORE_SITE_PATH));

		if (false) {
			// TODO: Reactivate as soon as a sane HAR is uploaded.
			conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, PROJECTS_HAR_PATH);
		}

		// we read the pack files from the pre-generated index file because
		// executing `hadoop fs -ls /tmp/repos/` or recursively searching in
		// the HAR file is terribly slow

		Collection<Path> packFilePaths = new ArrayList<>();

		try(BufferedReader br = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path("/tmp/index")),
				Charsets.UTF_8))) {
			for (String line; (line = br.readLine()) != null;) {
				if (line.equals("")) {
					continue; // TODO: Delete this guard as soon as a sane HAR is uploaded.
				}

				// @formatter:off
				// sample line:
				// 5	repos/maven/objects/pack/pack-621f44a9430e5b6303c3580582160a3e53634553.pack
				// @formatter:on
				String[] record = line.split("\\s+");
				// see: Hadoop: The Definitive Guide, p. 78
				// @formatter:off
				// hadoop fs -lsr har://hdfs-localhost:8020/my/files.har/my/files/dir
				// @formatter:on

				// TODO: Delete the "substring" as soon as a sane HAR is uploaded.
				Path packPath = new Path(PROJECTS_HAR_PATH + "/" + record[1].substring("/tmp".length()));
				packFilePaths.add(packPath);
			}
		}
		return Joiner.on(",").join(packFilePaths);
	}

	static class GitTablePopulatorMapper extends GuiceMapper<Text, BytesWritable, Text, IntWritable> {
		private static final String PACK_PATH_REGEX = "(.+)objects/pack/pack-[a-f0-9]{40}.pack";

		FileSystem fileSystem; // Set in setup.

		final private Populator populator;
		final private GitPopulator gitWalker;

		// Optional because in MRMain, we have an injector that does not set
		// this property, and can't, because it doesn't have the counter
		// available.
		@Inject(optional = true)
		@Named(Constants.COUNTER_PROCESSED_FILES)
		private Counter processedFilesCounter;

		@Inject
		GitTablePopulatorMapper(@Java Populator populator, GitPopulator gitWalker) {
			this.populator = populator;
			this.gitWalker = gitWalker;
		}

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = new Configuration();
			conf.addResource(new Path(CORE_SITE_PATH));
			conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, PROJECTS_HAR_PATH);
			fileSystem = FileSystem.get(conf);
		}

		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			logger.info("Received: " + key);

			Matcher matcher = Pattern.compile(PACK_PATH_REGEX).matcher(key.toString());
			if (!matcher.matches()) {
				throw new RuntimeException("Something seems to be wrong with this input path: " + key);
			}
			String gitDirPath = matcher.group(1);
			// Sorted alphabetically. This means: old to new.

			String projectName = gitWalker.extractProjectName(key.toString());
			logger.info("Processing " + projectName);

			gitWalker.walk(fileSystem.open(new Path(gitDirPath + org.eclipse.jgit.lib.Constants.PACKED_REFS)),
					new ByteArrayInputStream(value.getBytes()), projectName);
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			if (populator != null) {
				populator.close();
			}
		}
	}
}
