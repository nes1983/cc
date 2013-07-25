package ch.unibe.scg.cc;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.Assert;
import org.junit.Test;

import ch.unibe.scg.cc.Annotations.Snippet2Functions;
import ch.unibe.scg.cc.Protos.CodeFile;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Protos.GitRepo;
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
public final class GitPopulatorTest {
	private static final String TESTREPO = "testrepo.zip";

	@Test
	public void testProjnameRegex() throws IOException {
		try(GitPopulator gitWalker = new GitPopulator(null, null, null)) {
			String fullPathString = "har://hdfs-haddock.unibe.ch/projects/testdata.har"
					+ "/apfel/.git/objects/pack/pack-b017c4f4e226868d8ccf4782b53dd56b5187738f.pack";
			String projName = gitWalker.extractProjectName(fullPathString);
			Assert.assertEquals("apfel", projName);

			fullPathString = "har://hdfs-haddock.unibe.ch/projects/dataset.har/dataset/sensei/objects/pack/pack-a33a3daca1573e82c6fbbc95846a47be4690bbe4.pack";
			projName = gitWalker.extractProjectName(fullPathString);
			Assert.assertEquals("sensei", projName);
		}
	}

	@Test
	public void testPopulate() throws IOException {
		Injector i = Guice.createInjector(new CCModule(), new JavaModule(), new InMemoryModule());
		walkRepo(i, parseZippedGit(TESTREPO));

		Iterable<Iterable<Cell<Project>>> projectPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Project>>() {})).partitions();
		assertThat(Iterables.size(projectPartitions), is(1));

		Iterable<Cell<Project>> projects = Iterables.getOnlyElement(projectPartitions);
		assertThat(Iterables.size(projects), is(1));

		PopulatorCodec codec = i.getInstance(PopulatorCodec.class);
		Project p = codec.project.decode(Iterables.getOnlyElement(projects));
		assertThat(p.getName(), is("testrepo.zip"));

		Iterable<Iterable<Cell<Version>>> versionPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Version>>() {})).partitions();
		assertThat(Iterables.size(versionPartitions), is(1));

		Iterable<Cell<Version>> versions = Iterables.getOnlyElement(versionPartitions);
		assertThat(Iterables.size(versions), is(1));
		Version v = codec.version.decode(Iterables.getOnlyElement(versions));
		assertThat(v.getName(), is("master"));
		assertThat(v.getProject(), is(p.getHash()));

		Iterable<Iterable<Cell<CodeFile>>> filePartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<CodeFile>>() {})).partitions();
		assertThat(Iterables.size(filePartitions), is(1));

		Iterable<Cell<CodeFile>> files = Iterables.getOnlyElement(filePartitions);
		assertThat(Iterables.size(files), is(1));
		CodeFile cf = codec.codeFile.decode(Iterables.getOnlyElement(files));
		assertThat(cf.getPath(), is("GitTablePopulatorTest.java"));
		assertThat(cf.getVersion(), is(v.getHash()));

		assertThat(cf.getContents().indexOf("package ch.unibe.scg.cc.mappers;"), is(0));

		Iterable<Iterable<Cell<Function>>> functionPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Function>>() {})).partitions();
		assertThat(Iterables.size(functionPartitions), is(1));

		Iterable<Cell<Function>> functions = Iterables.getOnlyElement(functionPartitions);
		assertThat(Iterables.size(functions), is(1));

		Function fn = codec.function.decode(Iterables.getOnlyElement(functions));
		assertThat(fn.getCodeFile(), is(cf.getHash()));
		assertThat(fn.getBaseLine(), is(10));
		assertThat(fn.getContents().indexOf("public void testProjname"), is(1));

		Iterable<Iterable<Cell<Snippet>>> snippetPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Snippet>>() {})).partitions();
		assertThat(Iterables.size(snippetPartitions), is(1)); // means we have only one function

		Iterable<Cell<Snippet>> snippets = Iterables.getOnlyElement(snippetPartitions);
		assertThat(Iterables.size(snippets), is(8));
		Snippet s0 = codec.snippet.decode(Iterables.get(snippets, 0));
		Snippet s1 = codec.snippet.decode(Iterables.get(snippets, 1));
		Snippet s7 = codec.snippet.decode(Iterables.get(snippets, 7));

		assertThat(s0.getFunction(), is(fn.getHash()));
		assertThat(s0.getLength(), is(5));
		assertThat(s0.getPosition(), is(0));
		assertThat(s1.getPosition(), is(1));
		assertThat(s1.getFunction(), is(fn.getHash()));
		assertThat(s7.getPosition(), is(7));
		assertThat(s7.getFunction(), is(fn.getHash()));

		Iterable<Iterable<Cell<Snippet>>> snippet2FuncsPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Snippet>>() {}, Snippet2Functions.class)).partitions();
		assertThat(Iterables.size(snippet2FuncsPartitions), is(24 - 2)); // 2 collisions

		Iterable<Cell<Snippet>> snippets2Funcs = Iterables.get(snippet2FuncsPartitions, 0);
		assertThat(Iterables.size(snippets2Funcs), is(1));
		assertThat(Iterables.get(snippets2Funcs, 0).columnKey, is(fn.getHash()));

		ByteString snippetHash = Iterables.get(snippets2Funcs, 0).rowKey;
		boolean found = false;
		for (Cell<Snippet> cs : snippets) {
			found |= codec.snippet.decode(cs).getHash().equals(snippetHash);
		}
		assertTrue("Expected the column key of function2snippet to be a snippet hash, but wasn't.", found);
	}

	@Test
	public void testPaperExampleFunction2Snippets() throws IOException {
		Injector i = Guice.createInjector(new CCModule(), new JavaModule(), new InMemoryModule());
		walkRepo(i, GitPopulatorTest.parseZippedGit("paperExample.zip"));

		PopulatorCodec codec = i.getInstance(PopulatorCodec.class);
		Iterable<Cell<Function>> funCells = Iterables.concat(i.getInstance(Key.get(new TypeLiteral<CellSource<Function>>() {})).partitions());
		assertThat(Iterables.size(funCells), is(15));
		Iterable<Function> funs = Codecs.decode(funCells, codec.function);

		Set<ByteString> funHashes = new HashSet<>();
		for (Function fun : funs) {
			funHashes.add(fun.getHash());
		}
		assertThat(funHashes.size(), is(9));
	}

	@Test
	public void testPaperExampleSnippet2Functions() throws IOException {
		Injector i = Guice.createInjector(new CCModule(), new JavaModule(), new InMemoryModule());
		walkRepo(i, GitPopulatorTest.parseZippedGit("paperExample.zip"));

		Iterable<Iterable<Cell<Snippet>>> function2snippetsPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Snippet>>() {})).partitions();
		// Num partitions is the number of functions. As per populator test, that's 9.
		assertThat(Iterables.size(function2snippetsPartitions), is(9));

		Iterable<Iterable<Cell<Snippet>>> snippet2FunctionsPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Snippet>>() {}, Snippet2Functions.class)).partitions();
		assertThat(Iterables.size(snippet2FunctionsPartitions), is(145));

		// Numbers are taken from paper. See table II.
		ByteString row03D8 = ByteString.copyFrom(new byte[] {0x03, (byte) 0xd8});
		Iterable<Cell<Snippet>> partition03D8 = null;
		for(Iterable<Cell<Snippet>> s2fPartition : snippet2FunctionsPartitions) {
			if(Iterables.get(s2fPartition, 0).rowKey.startsWith(row03D8)) {
				partition03D8 = s2fPartition;
				break;
			}
		}
		Assert.assertNotNull(partition03D8);
		assertThat(Iterables.size(partition03D8), is(2));

		Codec<Snippet> s2fCodec = i.getInstance(Key.get(new TypeLiteral<Codec<Snippet>>() {}, Snippet2Functions.class));
		Iterable<Snippet> s2fs = Codecs.decode(partition03D8, s2fCodec);
		assertThat(Math.abs(Iterables.get(s2fs, 0).getPosition() - Iterables.get(s2fs, 1).getPosition()), is(3));
	}

	static void walkRepo(Injector i, GitRepo repo) throws IOException {
		try(CellSink<Snippet> snippetCellSink = i.getInstance(
				Key.get(new TypeLiteral<CellSink<Snippet>>() {}, Snippet2Functions.class));
				GitPopulator gitWalker = i.getInstance(GitPopulator.class)) {
			Sink<Snippet> snippetSink = Codecs.encode(snippetCellSink, i.getInstance(
					Key.get(new TypeLiteral<Codec<Snippet>>() {}, Snippet2Functions.class)));
			gitWalker.map(repo, Arrays.asList(repo), snippetSink);
		}
	}

	static GitRepo parseZippedGit(String pathToZip) throws IOException {
		try(ZipInputStream packFile = new ZipInputStream(GitPopulatorTest.class.getResourceAsStream(pathToZip));
				ZipInputStream packedRefs = new ZipInputStream(GitPopulatorTest.class.getResourceAsStream(pathToZip))) {
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
			return GitRepo.newBuilder()
					.setProjectName(pathToZip)
					.setPackFile(ByteString.readFrom(packFile))
					.setPackRefs(ByteString.readFrom(packedRefs))
					.build();
		}
	}
}