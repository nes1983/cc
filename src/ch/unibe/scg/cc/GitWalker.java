package ch.unibe.scg.cc;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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

import ch.unibe.scg.cc.Populator.ProjectRegistrar;
import ch.unibe.scg.cc.Populator.VersionRegistrar;

import com.google.common.io.Files;

/** GitWalker walks Git repositories and hands their files to the {@link Populator}. */
public class GitWalker {
	final static Logger logger = Logger.getLogger(GitWalker.class.getName());
	final static private Pattern projectNameRegexNonBare = Pattern.compile(".+?/([^/]+)/.git/.*");
	final static private Pattern projectNameRegexBare = Pattern.compile(".+?/([^/]+)/objects/.*");

	final private CharsetDetector charsetDetector;
	final private Populator populator;

	@Inject
	GitWalker(CharsetDetector charsetDetector, Populator populator) {
		this.charsetDetector = charsetDetector;
		this.populator = populator;
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
	public void walk(InputStream packedRefs, InputStream packFile, String projectName) throws IOException {
		List<PackedRef> tags = new PackedRefParser().parse(packedRefs);

		File tdir = null;
		try(ProjectRegistrar projectRegistrar = populator.makeProjectRegistrar(projectName)) {
			tdir = Files.createTempDir();
			FileRepository r = new FileRepository(tdir);
			r.create(true);
			PackParser pp = r.newObjectInserter().newPackParser(packFile);
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
					logger.warning("MissingObjectException in " + projectName + " : " + moe);
				}
			}
		} finally {
			if (tdir != null) {
				boolean ok = tdir.delete();
				if (!ok) {
					logger.warning("Failed to delete " + tdir);
				}
			}
		}
		logger.info("Finished processing: " + projectName);
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
}
