package ch.unibe.scg.cc.mappers;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;
import javax.inject.Named;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.transport.PackParser;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.junit.Test;

import ch.unibe.scg.cc.Frontend;
import ch.unibe.scg.cc.Java;
import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.RealProjectFactory;
import ch.unibe.scg.cc.activerecord.RealVersionFactory;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.git.PackedRef;
import ch.unibe.scg.cc.git.PackedRefParser;
import ch.unibe.scg.cc.mappers.Histogram.HistogramReducer;
import ch.unibe.scg.cc.mappers.TablePopulator.CharsetDetector;
import ch.unibe.scg.cc.mappers.inputformats.GitPathInputFormat;
import ch.unibe.scg.cc.util.WrappedRuntimeException;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

public class GitTablePopulator implements Runnable {
	static Logger logger = Logger.getLogger(GitTablePopulator.class);
	private static final String CORE_SITE_PATH = "/etc/hadoop/conf/core-site.xml";
	private static final String MAP_MEMORY = "2000";
	private static final String MAPRED_CHILD_JAVA_OPTS = "-Xmx2000m";
	private static final String REGEX_PACKFILE = "(.+)objects/pack/pack-[a-f0-9]{40}\\.pack";
	private static final String PROJECTS_HAR_PATH = "har://hdfs-haddock.unibe.ch/projects/testdata.har"; // projects.har
	private static final String PROJECTS_FOLDER_PATH = "."; // . for all folders
	private static final long MAX_PACK_FILESIZE_BYTES = 100000000;
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
			config.set(MRJobConfig.TASK_TIMEOUT, "86400000");
			config.set(MRJobConfig.MAP_MEMORY_MB, MAP_MEMORY);
			config.set(MRJobConfig.MAP_JAVA_OPTS, MAPRED_CHILD_JAVA_OPTS);
			config.setClass(Job.INPUT_FORMAT_CLASS_ATTR, GitPathInputFormat.class, InputFormat.class);
			config.setClass(Job.OUTPUT_FORMAT_CLASS_ATTR, NullOutputFormat.class, OutputFormat.class);
			config.setClass(Job.COMBINE_CLASS_ATTR, HistogramReducer.class, Reducer.class);
			String inputPaths = getInputPaths();
			config.set(FileInputFormat.INPUT_DIR, inputPaths);

			logger.debug("found: " + inputPaths);
			logger.debug("yyy wait for completion");
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
		conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, PROJECTS_HAR_PATH);
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(PROJECTS_FOLDER_PATH);

		Queue<Path> packFilePaths = new LinkedList<Path>();
		logger.debug("yyy start finding pack files " + path);
		findPackFilePaths(fs, path, packFilePaths);

		Joiner joiner = Joiner.on(",");
		return joiner.join(packFilePaths);

	}

	/**
	 * @param listToFill
	 *            result parameter
	 */
	private void findPackFilePaths(FileSystem fs, Path path, Queue<Path> listToFill) throws IOException {
		FileStatus[] fstatus = fs.listStatus(path);
		for (FileStatus f : fstatus) {
			Path p = f.getPath();
			logger.debug("yyy scanning: " + f.getPath() + " || " + f.getPath().getName());
			if (f.isFile() && f.getPath().toString().matches(REGEX_PACKFILE) && f.getLen() <= MAX_PACK_FILESIZE_BYTES) {
				listToFill.add(p);
			} else if (f.isDirectory())
				findPackFilePaths(fs, p, listToFill);
		}
	}

	public static class GitTablePopulatorMapper extends GuiceMapper<Text, BytesWritable, Text, IntWritable> {
		private static final int MAX_TAGS_TO_PARSE = 15;

		@Inject
		GitTablePopulatorMapper(@Java Frontend javaFrontend, @Named("versions") HTable versions,
				@Named("files") HTable files, @Named("functions") HTable functions, @Named("facts") HTable facts,
				@Named("strings") HTable strings, RealProjectFactory projectFactory, RealVersionFactory versionFactory,
				CharsetDetector charsetDetector) {
			super();
			this.javaFrontend = javaFrontend;
			this.versions = versions;
			this.files = files;
			this.functions = functions;
			this.facts = facts;
			this.strings = strings;
			this.projectFactory = projectFactory;
			this.versionFactory = versionFactory;
			this.charsetDetector = charsetDetector;
		}

		final Frontend javaFrontend;
		final HTable versions, files, functions, facts, strings;
		final RealProjectFactory projectFactory;
		final RealVersionFactory versionFactory;
		final CharsetDetector charsetDetector;

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			String packFilePath = key.toString();
			logger.debug("yyy RECEIVED: " + packFilePath);
			InputStream packFileStream = new ByteArrayInputStream(value.getBytes());
			DfsRepositoryDescription desc = new DfsRepositoryDescription(packFilePath);
			InMemoryRepository r = new InMemoryRepository(desc);

			Configuration conf = new Configuration();
			conf.addResource(new Path(CORE_SITE_PATH));
			conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, PROJECTS_HAR_PATH);
			FileSystem fileSystem = FileSystem.get(conf);

			PackParser pp = r.newObjectInserter().newPackParser(packFileStream);
			pp.parse(null);

			RevWalk revWalk = new RevWalk(r);

			PackedRefParser prp = new PackedRefParser();
			Pattern pattern = Pattern.compile("(.+)objects/pack/pack-[a-f0-9]{40}.pack");
			Matcher matcher = pattern.matcher(key.toString());
			if (!matcher.matches())
				throw new RuntimeException("Something seems to be wrong with this input path: " + key.toString());
			String gitDirPath = matcher.group(1);
			String packedRefsPath = gitDirPath + Constants.PACKED_REFS;

			FSDataInputStream ins = fileSystem.open(new Path(packedRefsPath));
			List<PackedRef> pr = prp.parse(ins);

			String projectName = getProjName(packFilePath);
			logger.debug("PROCESSING: " + packFilePath);
			int tagCount = pr.size();
			if (tagCount > MAX_TAGS_TO_PARSE) {
				int toIndex = tagCount - 1;
				int fromIndex = (tagCount - MAX_TAGS_TO_PARSE) < 0 ? 0 : (tagCount - MAX_TAGS_TO_PARSE);
				pr = pr.subList(fromIndex, toIndex);
			}
			pr = Lists.reverse(pr);
			Iterator<PackedRef> it = pr.iterator();
			int processedTagsCounter = 0;
			while (it.hasNext() && processedTagsCounter < MAX_TAGS_TO_PARSE) {
				PackedRef paref = it.next();
				String tag = paref.getName();
				logger.debug("WALK TAG: " + tag);

				revWalk.dispose();
				RevCommit commit;
				try {
					commit = revWalk.parseCommit(paref.getKey());
				} catch (MissingObjectException e) {
					logger.debug("ERROR in file " + packFilePath + ": " + e.getMessage());
					continue;
				}
				RevTree tree = commit.getTree();
				TreeWalk treeWalk = new TreeWalk(r);
				treeWalk.addTree(tree);
				treeWalk.setRecursive(true);
				if (!treeWalk.next()) {
					return;
				}
				while (treeWalk.next()) {
					ObjectId objectId = treeWalk.getObjectId(0);
					try {
						String content = getContent(r, objectId);
						String filePath = treeWalk.getPathString();
						if (!filePath.endsWith(".java"))
							continue;
						String fileName = filePath.lastIndexOf('/') == -1 ? filePath : filePath.substring(filePath
								.lastIndexOf('/') + 1);
						CodeFile codeFile = register(content, fileName);
						Version version = register(filePath, codeFile);
						register(projectName, version, tag);
					} catch (MissingObjectException moe) {
						System.out.println("twn MissingObjectException: " + moe);
					}
				}
				processedTagsCounter++;
			}
			logger.debug("svd FINISHED PROCESSING " + packFilePath);
		}

		private String getProjName(String packFilePath) {
			Matcher m = Pattern.compile(".+/(.+)/.git/.+").matcher(packFilePath);
			if (!m.matches()) {
				return packFilePath;
			}
			return m.group(1);
		}

		private String getContent(Repository repository, ObjectId objectId) throws MissingObjectException, IOException {
			ObjectLoader loader = repository.open(objectId);
			InputStream inputStream = loader.openStream();
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
			StringBuilder stringBuilder = new StringBuilder();
			String line = null;
			while ((line = bufferedReader.readLine()) != null) {
				stringBuilder.append(line + "\n");
			}
			bufferedReader.close();
			return stringBuilder.toString();
		}

		private CodeFile register(String content, String fileName) throws IOException {
			CodeFile codeFile = javaFrontend.register(content, fileName);
			return codeFile;
		}

		private Version register(String filePath, CodeFile codeFile) throws IOException {
			Version version = versionFactory.create(filePath, codeFile);
			javaFrontend.register(version);
			return version;
		}

		private void register(String projectName, Version version, String tag) throws IOException {
			Project proj = projectFactory.create(projectName, version, tag);
			javaFrontend.register(proj);
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			versions.flushCommits();
			files.flushCommits();
			functions.flushCommits();
			facts.flushCommits();
			strings.flushCommits();
		}
	}

	public static class GitTablePopulatorTest {
		@Test
		public void testProjnameRegex() {
			GitTablePopulatorMapper gtpm = new GitTablePopulatorMapper(null, null, null, null, null, null, null, null,
					null);
			String fullPathString = "har://hdfs-haddock.unibe.ch/projects/testdata.har"
					+ "/apfel/.git/objects/pack/pack-b017c4f4e226868d8ccf4782b53dd56b5187738f.pack";
			String projName = gtpm.getProjName(fullPathString);
			Assert.assertEquals("apfel", projName);
		}
	}
}
