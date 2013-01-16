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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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

import ch.unibe.scg.cc.Frontend;
import ch.unibe.scg.cc.Java;
import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.RealProjectFactory;
import ch.unibe.scg.cc.activerecord.RealVersionFactory;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.git.PackedRef;
import ch.unibe.scg.cc.git.PackedRefParser;
import ch.unibe.scg.cc.mappers.TablePopulator.CharsetDetector;
import ch.unibe.scg.cc.mappers.inputformats.GitPathInputFormat;
import ch.unibe.scg.cc.util.WrappedRuntimeException;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class GitTablePopulator implements Runnable {
	static Logger logger = Logger.getLogger(GitTablePopulator.class);
	private static final String CORE_SITE_PATH = "/etc/hadoop/conf/core-site.xml";
	private static final String MAP_MEMORY = "2000";
	private static final String REDUCE_MEMORY = "2000";
	private static final String MAPRED_CHILD_JAVA_OPTS = "-Xmx2000m";
	private static final String REGEX_PACKFILE = "(.+)objects/pack/pack-[a-f0-9]{40}\\.pack";
	private static final String PROJECTS_PATH = "/project-clone-detector/testdata";
	private static final long MAX_PACK_FILESIZE_BYTES = 100000000;
	final HBaseWrapper hbaseWrapper;

	@Inject
	GitTablePopulator(HBaseWrapper hbaseWrapper) {
		this.hbaseWrapper = hbaseWrapper;
	}

	public void run() {
		try {
			Job job = hbaseWrapper.createMapJob("gitPopulate", GitTablePopulator.class, "GitTablePopulatorMapper",
					Text.class, IntWritable.class);
			job.setInputFormatClass(GitPathInputFormat.class);
			job.setOutputFormatClass(NullOutputFormat.class);
			job.getConfiguration().set("mapred.child.java.opts", MAPRED_CHILD_JAVA_OPTS);
			job.getConfiguration().set("mapreduce.map.memory.mb", MAP_MEMORY);
			job.getConfiguration().set("mapreduce.reduce.memory.mb", REDUCE_MEMORY);
			String inputPaths = getInputPaths();
			FileInputFormat.addInputPaths(job, inputPaths);
			logger.debug("found: " + inputPaths);
			logger.debug("yyy wait for completion");
			job.waitForCompletion(true);
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
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(PROJECTS_PATH);

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
				@Named("strings") HTable strings, @Named("hashfactContent") HTable hashfactContent,
				RealProjectFactory projectFactory, RealVersionFactory versionFactory, CharsetDetector charsetDetector) {
			super();
			this.javaFrontend = javaFrontend;
			this.versions = versions;
			this.files = files;
			this.functions = functions;
			this.facts = facts;
			this.strings = strings;
			this.hashfactContent = hashfactContent;
			this.projectFactory = projectFactory;
			this.versionFactory = versionFactory;
			this.charsetDetector = charsetDetector;
		}

		final Frontend javaFrontend;
		final HTable versions, files, functions, facts, strings, hashfactContent;
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

			String projectName = packFilePath; // XXX
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
			functions.flushCommits();
			facts.flushCommits();
			hashfactContent.flushCommits();
			return codeFile;
		}

		private Version register(String filePath, CodeFile codeFile) throws IOException {
			Version version = versionFactory.create(filePath, codeFile);
			javaFrontend.register(version);
			files.flushCommits();
			return version;
		}

		private void register(String projectName, Version version, String tag) throws IOException {
			Project proj = projectFactory.create(projectName, version, tag);
			javaFrontend.register(proj);
			versions.flushCommits();
			strings.flushCommits();
		}
	}
}
