package ch.unibe.scg.cc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepository;
import org.eclipse.jgit.transport.PackParser;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathSuffixFilter;
import org.unibe.scg.cells.Mapper;

import ch.unibe.scg.cc.Populator.ProjectRegistrar;
import ch.unibe.scg.cc.Populator.VersionRegistrar;
import ch.unibe.scg.cc.Protos.GitRepo;
import ch.unibe.scg.cc.Protos.Snippet;

/** GitWalker walks Git repositories and hands their files to the {@link Populator}. */
public class GitPopulator implements Mapper<GitRepo, Snippet> {
	final static private Pattern projectNameRegexNonBare = Pattern.compile(".+?/([^/]+)/.git/.*");
	final static private Pattern projectNameRegexBare = Pattern.compile(".+?/([^/]+)/objects/.*");

	final private Logger logger;
	final private CharsetDetector charsetDetector;
	final private Populator populator;

	@Inject
	GitPopulator(CharsetDetector charsetDetector, Populator populator, Logger logger) {
		this.charsetDetector = charsetDetector;
		this.populator = populator;
		this.logger = logger;
	}

	private static  class PackedRefParser {
		final static Pattern pattern = Pattern.compile("([a-f0-9]{40}) refs\\/(?:tags|heads)\\/(.+)");

		public List<PackedRef> parse(InputStream ins) throws IOException {
			int ch;
			StringBuilder content = new StringBuilder();
			while ((ch = ins.read()) != -1) {
				content.append((char) ch);
			}

			return parse(content.toString());
		}

		private List<PackedRef> parse(String content) {
			List<PackedRef> list = new ArrayList<>();
			try (Scanner s = new Scanner(content)) {
				while (s.hasNextLine()) {
					String line = s.nextLine();
					Matcher m = pattern.matcher(line);
					if (m.matches()) {
						String sha = m.group(1);
						assert sha.length() == 40;
						ObjectId key = ObjectId.fromString(sha);
						String name = m.group(2);
						PackedRef pr = new PackedRef(key, name);
						list.add(pr);
					}
				}
			}
			return list;
		}
	}

	static class PackedRef {
		final ObjectId key;
		final String name;

		PackedRef(ObjectId key, String name) {
			this.key = key;
			this.name = name;
		}

		public ObjectId getKey() {
			return key;
		}

		public String getName() {
			return name;
		}
	}

	/** Processes the Git repository and hands the files to the {@link Populator}. */
	@Override
	public void map(GitRepo repo, Iterable<GitRepo> row, Sink<Snippet> sink) throws IOException {
		List<PackedRef> tags = new PackedRefParser().parse(repo.getPackRefs().newInput());

		Path tdir = null;
		try(ProjectRegistrar projectRegistrar = populator.makeProjectRegistrar(repo.getProjectName(), sink)) {
			tdir = Files.createTempDirectory(null);
			FileRepository r = new FileRepository(tdir.toFile());
			r.create(true);
			PackParser pp = r.newObjectInserter().newPackParser(repo.getPackFile().newInput());
			// ProgressMonitor set to null, so NullProgressMonitor will be used.
			pp.parse(null);

			for (PackedRef paref : tags) {
				logger.info("WALK TAG: " + paref.getName());

				try(VersionRegistrar vr = projectRegistrar.makeVersionRegistrar(paref.getName())) {
					TreeWalk treeWalk = new TreeWalk(r);
					treeWalk.addTree(new RevWalk(r).parseCommit(paref.getKey()).getTree());
					treeWalk.setRecursive(true);
					treeWalk.setFilter(PathSuffixFilter.create(".java"));

					while (treeWalk.next()) {
						// There's only one tree; it has index 0.
						ObjectId objectId = treeWalk.getObjectId(0);
						byte[] fileContents = treeWalk.getObjectReader().open(objectId).getBytes();
						vr.makeFileRegistrar().register(treeWalk.getPathString(),
								new String(fileContents, charsetDetector.charsetOf(fileContents)));
						// TODO processedFilesCounter.increment();
					}
				} catch (MissingObjectException moe) {
					logger.warning("MissingObjectException in " + repo.getProjectName() + " : " + moe);
				}
			}
		} finally {
			if (tdir != null) {
				try {
					removeRecursive(tdir);
				} catch(IOException e) {
					logger.warning("Failed to delete " + tdir + " because " + e);
				}
			}
		}
		logger.info("Finished processing: " + repo.getProjectName());
	}

	/** Taken from http://stackoverflow.com/questions/779519/delete-files-recursively-in-java/8685959#8685959 */
	private static void removeRecursive(Path path) throws IOException {
		Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
			@Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {
				// try to delete the file anyway, even if its attributes
				// could not be read, since delete-only access is
				// theoretically possible
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}

			@Override public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
				if (e == null) {
					Files.delete(dir);
					return FileVisitResult.CONTINUE;
				}
				throw e;
			}
		});
	}

	/**
	 * Heuristically extracts the name of the Project from the path to a git repo.
	 *
	 * @param packFilePath
	 *            full path of a pack file, possibly prefixed with a protocol.<br>
	 *            Example: har://bender.unibe.ch/ant/.git/objects/pack/pack-389c04f6e54ffd737e8b4a7448d5a4d3374a7c29.pack
	 */
	public String extractProjectName(String packFilePath) {
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

	@Override
	public void close() throws IOException {
		if (populator != null) {
			populator.close();
		}
	}
}
