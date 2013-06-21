package ch.unibe.scg.cc.mappers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Named;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepository;
import org.eclipse.jgit.transport.PackParser;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathSuffixFilter;

import ch.unibe.scg.cc.Frontend;
import ch.unibe.scg.cc.Java;
import ch.unibe.scg.cc.WrappedRuntimeException;
import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.ProjectFactory;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.activerecord.VersionFactory;
import ch.unibe.scg.cc.git.PackedRef;
import ch.unibe.scg.cc.git.PackedRefParser;
import ch.unibe.scg.cc.mappers.TablePopulator.CharsetDetector;
import ch.unibe.scg.cc.mappers.inputformats.GitPathInputFormat;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.inject.Inject;

public class GitTablePopulator implements Runnable {
	static Logger logger = Logger.getLogger(GitTablePopulator.class.getName());
	private static final String CORE_SITE_PATH = "/etc/hadoop/conf/core-site.xml";
	private static final String MAP_MEMORY = "4000";
	private static final String MAPRED_CHILD_JAVA_OPTS = "-Xmx3300m";
	// num_projects: projects.har 405 | testdata.har: 2 | dataset.har 2246
	/** needs to correspond with the path defined in DataFetchPipeline.sh */
	private static final String PROJECTS_HAR_PATH = "har://hdfs-haddock.unibe.ch/projects/dataset.har";
	/** set LOCAL_FOLDER_NAME to the same value as in RepoCloner.rb */
	private static final String LOCAL_FOLDER_NAME = "repos";
	final MRWrapper mrWrapper;

	@Inject
	GitTablePopulator(MRWrapper mrWrapper) {
		this.mrWrapper = mrWrapper;
	}

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
			config.setClass(Job.INPUT_FORMAT_CLASS_ATTR, GitPathInputFormat.class, InputFormat.class);
			config.setClass(Job.OUTPUT_FORMAT_CLASS_ATTR, NullOutputFormat.class, OutputFormat.class);
			String inputPaths = getInputPaths();
			config.set(FileInputFormat.INPUT_DIR, inputPaths);

			logger.finer("Input paths: " + inputPaths);
			mrWrapper.launchMapReduceJob("gitPopulate", config, Optional.<String> absent(), Optional.<String> absent(),
					null, GitTablePopulatorMapper.class.getName(), Optional.<String> absent(), Text.class,
					IntWritable.class);
		} catch (IOException e) {
			throw new WrappedRuntimeException(e);
		} catch (InterruptedException e) {
			throw new WrappedRuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new WrappedRuntimeException(e);
		}
	}

	private String getInputPaths() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path(CORE_SITE_PATH));
//		conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, PROJECTS_HAR_PATH);

		// we read the pack files from the pre-generated index file because
		// executing `hadoop fs -ls /tmp/repos/` or recursively searching in
		// the HAR file is terribly slow
		BufferedReader br = new BufferedReader(
				new InputStreamReader(FileSystem.get(conf).open(new Path("/tmp/index"))));
		Collection<Path> packFilePaths = Lists.newArrayList();
		String line;
		while ((line = br.readLine()) != null) {
			if (line.equals("")) {
				continue; // XXX This just shouldn't happen.
			}

			// @formatter:off
			// sample line:
			// 5	repos/maven/objects/pack/pack-621f44a9430e5b6303c3580582160a3e53634553.pack
			// @formatter:on
			String[] record = line.split("\\s+");
			int fileSize = Integer.parseInt(record[0]);
			// see: Hadoop: The Definitive Guide, p. 78
			// @formatter:off
			// hadoop fs -lsr har://hdfs-localhost:8020/my/files.har/my/files/dir
			// @formatter:on
			String packPath = PROJECTS_HAR_PATH + "/" + record[1].substring("/tmp".length()); //XXX remove substr
			if (fileSize > MAX_PACK_FILESIZE_MB) {
				logger.warning(packPath + " exceeded MAX_PACK_FILESIZE_MB and won't be processed.");
				continue;
			}
			packFilePaths.add(new Path(packPath));
		}
		return Joiner.on(",").join(packFilePaths);
	}

	public static class GitTablePopulatorMapper extends GuiceMapper<Text, BytesWritable, Text, IntWritable> {
		private static final int MAX_TAGS_TO_PARSE = 15;

		// Optional because in MRMain, we have an injector that does not set
		// this property, and can't, because it doesn't have the counter
		// available.
		@Inject(optional = true)
		@Named(GuiceResource.COUNTER_PROCESSED_FILES)
		Counter processedFilesCounter;

		@Inject
		GitTablePopulatorMapper(@Java Frontend javaFrontend, @Named("project2version") HTable project2version,
				@Named("version2file") HTable version2file, @Named("file2function") HTable file2function,
				@Named("function2snippet") HTable function2snippet, @Named("strings") HTable strings,
				ProjectFactory projectFactory, VersionFactory versionFactory, CharsetDetector charsetDetector) {
			this.javaFrontend = javaFrontend;
			this.project2version = project2version;
			this.version2file = version2file;
			this.file2function = file2function;
			this.function2snippet = function2snippet;
			this.strings = strings;
			this.projectFactory = projectFactory;
			this.versionFactory = versionFactory;
			this.charsetDetector = charsetDetector;
		}

		final Frontend javaFrontend;
		final HTable project2version, version2file, file2function, function2snippet, strings;
		final ProjectFactory projectFactory;
		final VersionFactory versionFactory;
		final CharsetDetector charsetDetector;
		final Pattern projectNameRegexNonBare = Pattern.compile(".+?/([^/]+)/.git/.*");
		final Pattern projectNameRegexBare = Pattern.compile(".+?/([^/]+)/objects/.*");

		FileSystem fileSystem;


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

			Matcher matcher = Pattern.compile("(.+)objects/pack/pack-[a-f0-9]{40}.pack").matcher(key.toString());
			if (!matcher.matches()) {
				throw new RuntimeException("Something seems to be wrong with this input path: " + key);
			}
			String gitDirPath = matcher.group(1);
			// Sorted alphabetically. This means: old to new.

			String projectName = getProjName(key.toString());
			logger.info("Processing " + projectName);

			mapRepo(fileSystem.open(new Path(gitDirPath + Constants.PACKED_REFS)),
					new ByteArrayInputStream(value.getBytes()), projectName);
		}

		void mapRepo(InputStream packedRefs, InputStream packFile, String projectName) throws IOException {
			List<PackedRef> tags = new PackedRefParser().parse(packedRefs);

			File tdir = null;
			try {
				tdir = Files.createTempDir();
				FileRepository r = new FileRepository(tdir);
				r.create(true);
				PackParser pp = r.newObjectInserter().newPackParser(packFile);
				// ProgressMonitor set to null, so NullProgressMonitor will be used.
				pp.parse(null);

				tags = Lists.reverse(tags).subList(0, Math.min(tags.size(), MAX_TAGS_TO_PARSE));
				for (PackedRef paref : tags) {
					logger.info("WALK TAG: " + paref.getName());

					try {
						TreeWalk treeWalk = new TreeWalk(r);
						treeWalk.addTree(new RevWalk(r).parseCommit(paref.getKey()).getTree());
						treeWalk.setRecursive(true);
						treeWalk.setFilter(PathSuffixFilter.create(".java"));

						while (treeWalk.next()) {
							ObjectId objectId = treeWalk.getObjectId(0); // There's only one tree; it has index 0.
							byte[] bytes = treeWalk.getObjectReader().open(objectId).getBytes();
							String content = new String(bytes, charsetDetector.charsetOf(bytes));
							String fileName = new File(treeWalk.getPathString()).getName();
							CodeFile codeFile = register(content, fileName);
							Version version = register(treeWalk.getPathString(), codeFile);
							register(projectName, version, paref.getName());
							processedFilesCounter.increment(1);
						}
					} catch (MissingObjectException moe) {
						logger.warning("MissingObjectException in " + projectName + " : " + moe);
					}
				}
			} finally {
				if (tdir != null) {
					tdir.delete();
				}
			}
			logger.info("Finished processing: " + projectName);
		}

		String getProjName(String packFilePath) {
			Matcher m = projectNameRegexNonBare.matcher(packFilePath);
			if (m.matches()) {
				return m.group(1);
			}

			m = projectNameRegexBare.matcher(packFilePath);
			if (m.matches()) {
				return m.group(1);
			}

			logger.warning("Could not simplify project name " + packFilePath);
			// Use URI as project name.
			return packFilePath;
		}

		private CodeFile register(String content, String fileName) {
			return javaFrontend.register(content, fileName);
		}

		private Version register(String filePath, CodeFile codeFile) {
			Version version = versionFactory.create(checkNotNull(filePath, "filePath"), checkNotNull(codeFile, "codeFile"));
			javaFrontend.register(version);
			return version;
		}

		private void register(String projectName, Version version, String tag) {
			javaFrontend.register(projectFactory.create(projectName, version, tag));
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			javaFrontend.close();
			project2version.flushCommits();
			version2file.flushCommits();
			file2function.flushCommits();
			function2snippet.flushCommits();
			strings.flushCommits();
		}
	}
}
