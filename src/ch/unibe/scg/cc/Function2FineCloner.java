package ch.unibe.scg.cc;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.CloneGroup;
import ch.unibe.scg.cc.Protos.CloneGroup.Builder;
import ch.unibe.scg.cc.Protos.CodeFile;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Protos.Occurrence;
import ch.unibe.scg.cc.Protos.Project;
import ch.unibe.scg.cc.Protos.Version;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;
import ch.unibe.scg.cells.LookupTable;
import ch.unibe.scg.cells.Mapper;
import ch.unibe.scg.cells.Sink;

import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Closer;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;

/** Expand clones and filter clones down to the clones that aren't spam. */
class Function2FineCloner implements Mapper<Clone, CloneGroup> {
	final private CloneExpander cloneExpander;
	final private LookupTable<Str<Function>> cloneLoader;
	final private SpamDetector spamDetector;
	final private Logger logger;
	final private StringOfLinesFactory stringOfLinesFactory;

	final private LookupTable<CodeFile> filesTab;
	final private LookupTable<Version> versionsTab;
	final private LookupTable<Project> projectsTab;
	final private LookupTable<Function> functionsTab;
	final private LookupTable<Str<Function>> funStringsTab;

	@Inject
	Function2FineCloner(StringOfLinesFactory stringOfLinesFactory, LookupTable<CodeFile> filesTab,
			LookupTable<Version> versionsTab, LookupTable<Project> projectsTab,
			LookupTable<Str<Function>> funStringsTab, Logger logger, CloneExpander cloneExpander,
			LookupTable<Str<Function>> cloneLoader, SpamDetector spamDetector, LookupTable<Function> functionsTab) {
		this.stringOfLinesFactory = stringOfLinesFactory;
		this.filesTab = filesTab;
		this.versionsTab = versionsTab;
		this.projectsTab = projectsTab;
		this.funStringsTab = funStringsTab;
		this.cloneExpander = cloneExpander;
		this.cloneLoader = cloneLoader;
		this.spamDetector = spamDetector;
		this.logger = logger;
		this.functionsTab = functionsTab;
	}

	@Override
	public void map(Clone first, Iterable<Clone> row, Sink<CloneGroup> sink) throws IOException {
		int from = Integer.MAX_VALUE;
		int to = Integer.MIN_VALUE;

		int commonness = 0;
		Builder cloneGroupBuilder = CloneGroup.newBuilder();

		ByteString fun = first.getThisSnippet().getFunction();
		Occurrence template = Occurrence.newBuilder().build(); // TODO: fill in
																// more.
		cloneGroupBuilder.addAllOccurrences(findOccurrences(fun, template));

		Collection<Clone> clones = cloneExpander.expandClones(row);

		for (Clone c : clones) {
			// TODO: Counters are missing.
			if (spamDetector.isSpamByParameters(spamDetector.extractFeatureVector(
					stringOfLinesFactory.make(
							Iterables.getOnlyElement(cloneLoader.readRow(c.getThisSnippet().getFunction())).contents)
							.getLines(c.getThisSnippet().getPosition(), c.getThisSnippet().getLength()),
					stringOfLinesFactory.make(
							Iterables.getOnlyElement(cloneLoader.readRow(c.getThatSnippet().getFunction())).contents)
							.getLines(c.getThatSnippet().getPosition(), c.getThatSnippet().getLength())))) {
				return;
			}

			checkState(c.getThisSnippet().getFunction().equals(fun),
					"The function hash key did not match one of the clones. Clone says: "
							+ BaseEncoding.base16().encode(c.getThisSnippet().getFunction().toByteArray())
							+ " reduce key: " + BaseEncoding.base16().encode(fun.toByteArray()));

			if (!c.getThisSnippet().getFunction().equals(fun)) {
				throw new AssertionError("There is a clone in cloneProtobufs that doesn't match the input function "
						+ BaseEncoding.base16().encode(fun.toByteArray()));
			}

			Collection<Occurrence> occ = findOccurrences(c.getThatSnippet().getFunction(), template);
			cloneGroupBuilder.addAllOccurrences(occ);

			from = Math.min(from, c.getThisSnippet().getPosition());
			to = Math.max(to, c.getThisSnippet().getPosition() + c.getThisSnippet().getLength());
			commonness++;
		}

		if (commonness <= 0) {
			throw new AssertionError("commonness must be non-negative, but was " + commonness);
		}

		String functionString = Iterables.getOnlyElement(funStringsTab.readRow(fun)).contents;

		cloneGroupBuilder.setText(stringOfLinesFactory.make(functionString, '\n').getLines(from, to - from));

		sink.write(cloneGroupBuilder.build());
	}

	/** @return all occurrences of {@code functionKey} */
	private Collection<Occurrence> findOccurrences(ByteString functionKey, Occurrence template) {
		// TODO: Cacheing needed/?
		Collection<Occurrence> ret = new ArrayList<>();
		Iterable<Function> funs = readColumn(functionsTab, functionKey, "functions");

		for (Function fun : funs) {
			Iterable<CodeFile> files = readColumn(filesTab, fun.getCodeFile(), "files");

			for (CodeFile file : files) {
				Iterable<Version> versions = readColumn(versionsTab, file.getVersion(), "versions");

				for (Version version : versions) {
					Iterable<Project> projects = readColumn(projectsTab, version.getProject(), "projects");

					for (Project project : projects) {
						ret.add(Occurrence.newBuilder(template).setVersion(version).setCodeFile(file)
								.setProject(project).build());
					}
				}
			}
		}

		return ret;
	}

	private <T> Iterable<T> readColumn(LookupTable<T> tab, ByteString hash, String table) {
		Iterable<T> ret = tab.readColumn(hash);
		if (Iterables.isEmpty(ret)) {
			logger.severe("Found no " + table + " for hash "
					+ BaseEncoding.base16().encode(hash.toByteArray()).substring(0, 6));
		}
		return ret;
	}

	@Override
	public void close() throws IOException {
		try (Closer closer = Closer.create()) {
			closer.register(filesTab);
			closer.register(versionsTab);
			closer.register(projectsTab);
			closer.register(cloneLoader);
		}
	}
}
