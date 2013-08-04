package ch.unibe.scg.cc;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.CloneGroup;
import ch.unibe.scg.cc.Protos.CodeFile;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Protos.Occurrence;
import ch.unibe.scg.cc.Protos.Project;
import ch.unibe.scg.cc.Protos.Version;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;
import ch.unibe.scg.cells.LookupTable;
import ch.unibe.scg.cells.OfflineMapper;
import ch.unibe.scg.cells.Sink;
import ch.unibe.scg.cells.Source;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Closer;
import com.google.protobuf.ByteString;

class CloneGroupClusterer implements OfflineMapper<Clone, CloneGroup> {
	final private LookupTable<CodeFile> filesTab;
	final private LookupTable<Version> versionsTab;
	final private LookupTable<Project> projectsTab;
	final private LookupTable<Function> functionsTab;
	final private LookupTable<Str<Function>> funStringsTab;
	final private StringOfLinesFactory stringOfLinesFactory;

	final private Map<ByteString, Collection<Occurrence>> functionOccurrences = new HashMap<>();

	@Inject
	CloneGroupClusterer(@Annotations.Populator LookupTable<CodeFile> filesTab,
			@Annotations.Populator LookupTable<Version> versionsTab,
			@Annotations.Populator LookupTable<Project> projectsTab,
			@Annotations.Populator LookupTable<Function> functionsTab,
			@Annotations.Populator LookupTable<Str<Function>> funStringsTab,
			StringOfLinesFactory stringOfLinesFactory) {
		this.filesTab = filesTab;
		this.versionsTab = versionsTab;
		this.projectsTab = projectsTab;
		this.functionsTab = functionsTab;
		this.funStringsTab = funStringsTab;
		this.stringOfLinesFactory = stringOfLinesFactory;
	}

	@Override
	public void map(Source<Clone> in, Sink<CloneGroup> out) {
		Multimap<ByteString, Clone> hashToClone = HashMultimap.create();
		for (Iterable<Clone> row : in) {
			for (Clone c : row) {
				hashToClone.put(c.getThatSnippet().getFunction(), c);
				hashToClone.put(c.getThatSnippet().getFunction(), c);
			}
		}

		while (!hashToClone.isEmpty()) {
			ByteString start = hashToClone.entries().iterator().next().getKey();
			String cloneGroupText = extractText(hashToClone.get(start).iterator().next());

			Set<Occurrence> occurrences = new HashSet<>();
			for (Collection<Clone> cs = hashToClone.get(start); !cs.isEmpty(); cs = hashToClone.get(start)) {
				Clone c = cs.iterator().next();
				remove(hashToClone, c);
				extractConnectedComponent(hashToClone, c, occurrences);
			}

			out.write(CloneGroup.newBuilder()
					.addAllOccurrences(occurrences)
					.setText(cloneGroupText)
					.build());
		}
	}

	private String extractText(Clone anyClone) {
		String functionString = Iterables
				.getOnlyElement(funStringsTab.readRow(anyClone.getThisSnippet().getFunction())).contents;
		return stringOfLinesFactory.make(functionString, '\n').getLines(anyClone.getThisSnippet().getPosition(),
				anyClone.getThatSnippet().getLength());
	}

	private void extractConnectedComponent(Multimap<ByteString, Clone> hashToClone, Clone c, Set<Occurrence> out) {
		out.addAll(findOccurrences(c.getThatSnippet().getFunction()));
		out.addAll(findOccurrences(c.getThisSnippet().getFunction()));
		Collection<Clone> left = hashToClone.get(c.getThisSnippet().getFunction());
		for (Clone leftClone : left) {
			remove(hashToClone, leftClone);
			extractConnectedComponent(hashToClone, leftClone, out);
		}
		Collection<Clone> right = hashToClone.get(c.getThatSnippet().getFunction());
		for (Clone rightClone : right) {
			remove(hashToClone, rightClone);
			extractConnectedComponent(hashToClone, rightClone, out);
		}
	}

	private void remove(Multimap<ByteString, Clone> hashToClone, Clone leftClone) {
		boolean removedSomething = false;

		ByteString thisFun = leftClone.getThisSnippet().getFunction();
		if (hashToClone.containsEntry(thisFun, leftClone)) {
			hashToClone.remove(thisFun, leftClone);
			removedSomething = true;
		}

		ByteString thatFun = leftClone.getThatSnippet().getFunction();
		if (hashToClone.containsEntry(thatFun, leftClone)) {
			hashToClone.remove(thatFun, leftClone);
			removedSomething = true;
		}

		assert removedSomething;
	}

	/** @return all occurrences of {@code functionKey} */
	private Collection<Occurrence> findOccurrences(ByteString functionKey) {
		if (!functionOccurrences.containsKey(functionKey)) {
			ImmutableList.Builder<Occurrence> ret = ImmutableList.builder();

			Iterable<Function> funs = readColumn(functionsTab, functionKey, "functions");
			for (Function fun : funs) {

				Iterable<CodeFile> files = readColumn(filesTab, fun.getCodeFile(), "files");
				for (CodeFile file : files) {

					Iterable<Version> versions = readColumn(versionsTab, file.getVersion(), "versions");
					for (Version version : versions) {

						Iterable<Project> projects = readColumn(projectsTab, version.getProject(), "projects");
						for (Project project : projects) {
							ret.add(Occurrence.newBuilder().setFunction(fun).setCodeFile(file).setVersion(version)
									.setProject(project).build());
						}
					}
				}
			}

			functionOccurrences.put(functionKey, ret.build());
		}

		return functionOccurrences.get(functionKey);
	}

	private <T> Iterable<T> readColumn(LookupTable<T> tab, ByteString hash, String table) {
		Iterable<T> ret = tab.readColumn(hash);
		if (Iterables.isEmpty(ret)) {
			throw new RuntimeException("Found no " + table + " for hash "
					+ BaseEncoding.base16().encode(hash.toByteArray()).substring(0, 6));
		}
		return ret;
	}

	@Override
	public void close() throws IOException {
		try(Closer closer = Closer.create()) {
			closer.register(filesTab);
			closer.register(versionsTab);
			closer.register(projectsTab);
			closer.register(functionsTab);
			closer.register(funStringsTab);
		}
	}
}
