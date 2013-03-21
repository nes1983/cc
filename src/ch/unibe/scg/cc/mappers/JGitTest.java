package ch.unibe.scg.cc.mappers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.dfs.DfsObjDatabase;
import org.eclipse.jgit.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.transport.PackParser;
import org.eclipse.jgit.treewalk.TreeWalk;

import ch.unibe.scg.cc.WrappedRuntimeException;
import ch.unibe.scg.cc.git.PackedRef;
import ch.unibe.scg.cc.git.PackedRefParser;

public class JGitTest implements Runnable {
	static Logger logger = Logger.getLogger(JGitTest.class.getName());

	JGitTest() {
	}

	public void run() {
		try {
			DfsRepositoryDescription desc = new DfsRepositoryDescription("scribe-java");
			InMemoryRepository r = new InMemoryRepository(desc);

			Configuration conf = new Configuration();
			conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
			FileSystem fileSystem = FileSystem.get(conf);
			Path path = new Path(
					"/tmp/scribe-java/.git/objects/pack/pack-b0838be1c3b7b22ac27675baee3692c4d44f8cf0.pack");
			FSDataInputStream in = fileSystem.open(path);

			PackParser pp = r.newObjectInserter().newPackParser(in);
			pp.parse(null);

			Git git = Git.wrap(r);

			DfsObjDatabase db = r.getObjectDatabase();

			RevWalk revWalk = new RevWalk(r);

			PackedRefParser prp = new PackedRefParser();
			FSDataInputStream ins = fileSystem.open(new Path("/tmp/scribe-java/.git/packed-refs"));
			List<PackedRef> pr = prp.parse(ins);

			for (PackedRef paref : pr) {
				logger.finer("WALK TAG " + paref.getName());

				revWalk.dispose();
				RevCommit commit = revWalk.parseCommit(paref.getKey());
				RevTree tree = commit.getTree();
				TreeWalk treeWalk = new TreeWalk(r);
				treeWalk.addTree(tree);
				treeWalk.setRecursive(true);
				if (!treeWalk.next()) {
					return;
				}
				while (treeWalk.next()) {
					ObjectId objectId = treeWalk.getObjectId(0);
					ObjectLoader loader = r.open(objectId);

					logger.finer(Constants.typeString(loader.getType()) + ": " + objectId + " "
							+ treeWalk.getPathString());
					// logger.finer(getContent(r, objectId));
				}
			}
		} catch (Exception e) {
			logger.finer("ABSTURZ...");
			throw new WrappedRuntimeException(e);
		}
	}

	private static String getContent(Repository repository, ObjectId objectId) throws MissingObjectException,
			IOException {
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
}
