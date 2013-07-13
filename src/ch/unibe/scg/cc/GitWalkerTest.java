package ch.unibe.scg.cc;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.Assert;
import org.junit.Test;

import ch.unibe.scg.cc.Annotations.Function2Snippets;
import ch.unibe.scg.cc.Protos.CodeFile;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Protos.Project;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.Protos.Version;
import ch.unibe.scg.cc.javaFrontend.JavaModule;

import com.google.common.collect.Iterables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.protobuf.ByteString;

@SuppressWarnings("javadoc")
public final class GitWalkerTest {
	private static final String TESTREPO = "testrepo.zip";

	@Test
	public void testProjnameRegex() {
		GitWalker gitWalker = new GitWalker(null, null);
		String fullPathString = "har://hdfs-haddock.unibe.ch/projects/testdata.har"
				+ "/apfel/.git/objects/pack/pack-b017c4f4e226868d8ccf4782b53dd56b5187738f.pack";
		String projName = gitWalker.extractProjectName(fullPathString);
		Assert.assertEquals("apfel", projName);

		fullPathString = "har://hdfs-haddock.unibe.ch/projects/dataset.har/dataset/sensei/objects/pack/pack-a33a3daca1573e82c6fbbc95846a47be4690bbe4.pack";
		projName = gitWalker.extractProjectName(fullPathString);
		Assert.assertEquals("sensei", projName);
	}

	@Test
	public void testPopulate() throws IOException {
		Injector i = Guice.createInjector(new CCModule(), new JavaModule(), new InMemoryModule());
		GitWalker gitWalker = i.getInstance(GitWalker.class);

		try (ZipInputStream packFile = new ZipInputStream(GitWalkerTest.class.getResourceAsStream(TESTREPO));
				ZipInputStream packedRefs = new ZipInputStream(
						GitWalkerTest.class.getResourceAsStream(TESTREPO))) {
			for (ZipEntry entry; (entry = packFile.getNextEntry()) != null;) {
				if (entry.getName().endsWith(".pack")) {
					break;
				}
			}

			for (ZipEntry entry; (entry = packedRefs.getNextEntry()) != null;) {
				if (entry.getName().endsWith("packed-refs")) {
					break;
				}
			}

			gitWalker.walk(packedRefs, packFile, "Captain Hook");
		}

		Iterable<Iterable<Cell<Project>>> projectPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Project>>() {})).partitions();
		assertThat(Iterables.size(projectPartitions), is(1));

		Iterable<Cell<Project>> projects = Iterables.getOnlyElement(projectPartitions);
		assertThat(Iterables.size(projects), is(1));

		PopulatorCodec cellCodec = i.getInstance(PopulatorCodec.class);
		Project p = cellCodec.decodeProject(Iterables.getOnlyElement(projects));
		assertThat(p.getName(), is("Captain Hook"));

		Iterable<Iterable<Cell<Version>>> versionPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Version>>() {})).partitions();
		assertThat(Iterables.size(versionPartitions), is(1));

		Iterable<Cell<Version>> versions = Iterables.getOnlyElement(versionPartitions);
		assertThat(Iterables.size(versions), is(1));
		Version v = cellCodec.decodeVersion(Iterables.getOnlyElement(versions));
		assertThat(v.getName(), is("master"));
		assertThat(v.getProject(), is(p.getHash()));

		Iterable<Iterable<Cell<CodeFile>>> filePartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<CodeFile>>() {})).partitions();
		assertThat(Iterables.size(filePartitions), is(1));

		Iterable<Cell<CodeFile>> files = Iterables.getOnlyElement(filePartitions);
		assertThat(Iterables.size(files), is(1));
		CodeFile cf = cellCodec.decodeCodeFile(Iterables.getOnlyElement(files));
		assertThat(cf.getPath(), is("GitTablePopulatorTest.java"));
		assertThat(cf.getVersion(), is(v.getHash()));

		assertThat(cf.getContents().indexOf("package ch.unibe.scg.cc.mappers;"), is(0));

		Iterable<Iterable<Cell<Function>>> functionPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Function>>() {})).partitions();
		assertThat(Iterables.size(functionPartitions), is(1));

		Iterable<Cell<Function>> functions = Iterables.getOnlyElement(functionPartitions);
		assertThat(Iterables.size(functions), is(1));

		Function fn = cellCodec.decodeFunction(Iterables.getOnlyElement(functions));
		assertThat(fn.getCodeFile(), is(cf.getHash()));
		assertThat(fn.getBaseLine(), is(10));
		assertThat(fn.getContents().indexOf("public void testProjname"), is(1));

		Iterable<Iterable<Cell<Snippet>>> snippetPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Snippet>>() {})).partitions();
		assertThat(Iterables.size(snippetPartitions), is(1));

		Iterable<Cell<Snippet>> snippets = Iterables.getOnlyElement(snippetPartitions);
		assertThat(Iterables.size(snippets), is(24));
		Snippet s0 = cellCodec.decodeSnippet(Iterables.get(snippets, 0));
		Snippet s1 = cellCodec.decodeSnippet(Iterables.get(snippets, 1));
		Snippet s7 = cellCodec.decodeSnippet(Iterables.get(snippets, 7));

		assertThat(s0.getFunction(), is(fn.getHash()));
		assertThat(s0.getLength(), is(5));
		assertThat(s0.getPosition(), is(0));
		assertThat(s1.getPosition(), is(1));
		assertThat(s1.getFunction(), is(fn.getHash()));
		assertThat(s7.getPosition(), is(7));
		assertThat(s7.getFunction(), is(fn.getHash()));

		Iterable<Cell<Snippet>> snippets2Funcs = Iterables.getOnlyElement(i.getInstance(
				Key.get(new TypeLiteral<CellSource<Snippet>>() {}, Function2Snippets.class)).partitions());
		assertThat(Iterables.size(snippets2Funcs), is(24));
		assertThat(Iterables.get(snippets2Funcs, 0).rowKey, is(fn.getHash()));

		ByteString snippetHash = Iterables.get(snippets2Funcs, 0).columnKey;
		boolean found = false;
		for (Cell<Snippet> cs : snippets) {
			found |= cellCodec.decodeSnippet(cs).getHash().equals(snippetHash);
		}
		assertTrue("Expected the column key of function2snippet to be a snippet hash, but wasn't.", found);
	}
}