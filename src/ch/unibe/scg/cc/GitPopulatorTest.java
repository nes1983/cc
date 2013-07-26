package ch.unibe.scg.cc;

import static com.google.common.io.BaseEncoding.base16;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.junit.Assert;
import org.junit.Test;
import org.unibe.scg.cells.Cell;
import org.unibe.scg.cells.CellSink;
import org.unibe.scg.cells.CellSource;
import org.unibe.scg.cells.Codec;
import org.unibe.scg.cells.Codecs;

import ch.unibe.scg.cc.Annotations.Snippet2Functions;
import ch.unibe.scg.cc.Protos.CloneType;
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
				Key.get(new TypeLiteral<CellSource<Project>>() {}));
		assertThat(Iterables.size(projectPartitions), is(1));

		Iterable<Cell<Project>> projects = Iterables.getOnlyElement(projectPartitions);
		assertThat(Iterables.size(projects), is(1));

		PopulatorCodec codec = i.getInstance(PopulatorCodec.class);
		Project p = codec.project.decode(Iterables.getOnlyElement(projects));
		assertThat(p.getName(), is("testrepo.zip"));

		Iterable<Iterable<Cell<Version>>> versionPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Version>>() {}));
		assertThat(Iterables.size(versionPartitions), is(1));

		Iterable<Cell<Version>> versions = Iterables.getOnlyElement(versionPartitions);
		assertThat(Iterables.size(versions), is(1));
		Version v = codec.version.decode(Iterables.getOnlyElement(versions));
		assertThat(v.getName(), is("master"));
		assertThat(v.getProject(), is(p.getHash()));

		Iterable<Iterable<Cell<CodeFile>>> filePartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<CodeFile>>() {}));
		assertThat(Iterables.size(filePartitions), is(1));

		Iterable<Cell<CodeFile>> files = Iterables.getOnlyElement(filePartitions);
		assertThat(Iterables.size(files), is(1));
		CodeFile cf = codec.codeFile.decode(Iterables.getOnlyElement(files));
		assertThat(cf.getPath(), is("GitTablePopulatorTest.java"));
		assertThat(cf.getVersion(), is(v.getHash()));

		assertThat(cf.getContents().indexOf("package ch.unibe.scg.cc.mappers;"), is(0));

		Iterable<Iterable<Cell<Function>>> functionPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Function>>() {}));
		assertThat(Iterables.size(functionPartitions), is(1));

		Iterable<Cell<Function>> functions = Iterables.getOnlyElement(functionPartitions);
		assertThat(Iterables.size(functions), is(1));

		Function fn = codec.function.decode(Iterables.getOnlyElement(functions));
		assertThat(fn.getCodeFile(), is(cf.getHash()));
		assertThat(fn.getBaseLine(), is(10));
		assertThat(fn.getContents().indexOf("public void testProjname"), is(1));

		Iterable<Iterable<Cell<Snippet>>> snippetPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Snippet>>() {}));
		assertThat(Iterables.size(snippetPartitions), is(1)); // means we have only one function

		Iterable<Cell<Snippet>> snippets = Iterables.getOnlyElement(snippetPartitions);
		assertThat(Iterables.size(snippets), is(24));
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
				Key.get(new TypeLiteral<CellSource<Snippet>>() {}, Snippet2Functions.class));
		assertThat(Iterables.size(snippet2FuncsPartitions), is(24 - 2)); // 2 collisions

		Iterable<Cell<Snippet>> snippets2Funcs = Iterables.get(snippet2FuncsPartitions, 0);
		assertThat(Iterables.size(snippets2Funcs), is(1));
		assertThat(Iterables.get(snippets2Funcs, 0).getColumnKey(), is(fn.getHash()));

		ByteString snippetHash = Iterables.get(snippets2Funcs, 0).getRowKey();
		boolean found = false;
		for (Cell<Snippet> cs : snippets) {
			found |= codec.snippet.decode(cs).getHash().equals(snippetHash);
		}
		assertTrue("Expected the column key of function2snippet to be a snippet hash, but wasn't.", found);
	}

	@Test
	public void testPaperExampleFile2Function() throws IOException {
		Injector i = Guice.createInjector(new CCModule(), new JavaModule(), new InMemoryModule());
		walkRepo(i, GitPopulatorTest.parseZippedGit("paperExample.zip"));

		PopulatorCodec codec = i.getInstance(PopulatorCodec.class);
		List<Iterable<Function>> decodedPartitions = new ArrayList<>();
		for (Iterable<Cell<Function>> partition : i.getInstance(Key.get(new TypeLiteral<CellSource<Function>>() {}))) {
			decodedPartitions.add(Codecs.decode(partition, codec.function));
		}

		Iterable<Function> funs = Iterables.concat(decodedPartitions);
		assertThat(Iterables.size(funs), is(15));

		Set<ByteString> funHashes = new HashSet<>();
		for (Function fun : funs) {
			funHashes.add(fun.getHash());
		}
		assertThat(funHashes.size(), is(9));

		Set<String> fileHashes = new HashSet<>();
		for (Iterable<Function> partition : decodedPartitions) {
			String cur = base16().encode(Iterables.get(partition, 0).getCodeFile().toByteArray());
			Assert.assertFalse(cur + fileHashes, fileHashes.contains(cur));
			fileHashes.add(cur);
			// Check that every partition shares the same file.
			for (Function f : partition) {
				assertThat(base16().encode(f.getCodeFile().toByteArray()), is(cur));
			}
		}
		assertThat(fileHashes.size(), is(3));
	}

	@Test
	public void testPaperExampleSnippet2Functions() throws IOException {
		Injector i = Guice.createInjector(new CCModule(), new JavaModule(), new InMemoryModule());
		walkRepo(i, GitPopulatorTest.parseZippedGit("paperExample.zip"));

		Iterable<Iterable<Cell<Snippet>>> snippet2FunctionsPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Snippet>>() {}, Snippet2Functions.class));
		assertThat(Iterables.size(snippet2FunctionsPartitions), is(145));

		// Numbers are taken from paper. See table II.
		ByteString row03D8 = ByteString.copyFrom(new byte[] {0x03, (byte) 0xd8});
		Iterable<Cell<Snippet>> partition03D8 = null;
		for(Iterable<Cell<Snippet>> s2fPartition : snippet2FunctionsPartitions) {
			if(Iterables.get(s2fPartition, 0).getRowKey().startsWith(row03D8)) {
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

	@Test
	public void testPaperExampleFunction2Snippets() throws IOException {
		Injector i = Guice.createInjector(new CCModule(), new JavaModule(), new InMemoryModule());
		walkRepo(i, GitPopulatorTest.parseZippedGit("paperExample.zip"));

		Iterable<Iterable<Cell<Snippet>>> function2snippetsPartitions = i.getInstance(
				Key.get(new TypeLiteral<CellSource<Snippet>>() {}));

		List<Iterable<Snippet>> decodedRows = new ArrayList<>();
		Codec<Snippet> codec = i.getInstance(PopulatorCodec.class).snippet;
		for (Iterable<Cell<Snippet>> partition : function2snippetsPartitions) {
			decodedRows.add(Codecs.decode(partition, codec));
		}

		// Num partitions is the number of functions. As per populator test, that's 9.
		assertThat(Iterables.size(function2snippetsPartitions), is(9));

		// We'll examine this row further in Function2RoughClones
		Iterable<Snippet> d618 = null;
		for (Iterable<Snippet> row : decodedRows) {
			if (base16().encode(Iterables.get(row, 0).getFunction().toByteArray()).startsWith("D618")) {
				d618 = row;
				break;
			}
		}
		assertNotNull(d618);
		assert d618 != null; // Null analysis insists ...

		List<String> snippetHashes = new ArrayList<>();
		for (Snippet s : d618) {
			if (s.getCloneType().equals(CloneType.GAPPED)) {
				snippetHashes.add(base16().encode(s.getHash().toByteArray()));
			}
		}

		// These snippets were only partially checked.
		assertThat(snippetHashes, is(d618SnippetHashes()));
	}

	static Collection<String> d618SnippetHashes() {
		return Arrays.asList("9721D2E18CFB9E67DD3787FA16426FF15FA4DDC3", "88895703E487880901E8A65520240A9242A329E2",
				"1FF04CEAED39AEEB97E965622549D309CC449082", "BB3E14556ABA10796131584A07979C50470B4DBA",
				"54F44DF1B74E5C820084644F002628098403C3F5", "ECBBB50DB74FB3CEA92496B0A8EBDD00CB026477",
				"E4449FB81BF8157AB4A4EA29B70F626C299571E3", "A0371539D00B078FE5EC14FEECF5D03E387C0995",
				"FC065CF873BF61F22B363F08D9CDC20726659250", "03D8952065410DFEB6F65F83EBA3AD70A1B9F3B3",
				"4FE1D1D7D18223F94D13CEB4A2DB2E30DF5CAE8D", "4FE1D1D7D18223F94D13CEB4A2DB2E30DF5CAE8D",
				"20A4E2A38A590E7D29978E7A3FF308EE15B6DC63", "6F4533745BDB2D84648440CE9D2826DECAEA72EE",
				"BFEA4FB989E4BB732B2D3C1DB4D2B522389D93A7", "ABB06C63965C7AFEFB98F464BB628AA84C6BAE50",
				"D0AF7CCDD23F96F74FA97CD329FA93FCF277E149", "C7511FD22F9017C9188B65BF5FC50CB53812FA18",
				"C46C6B63797890F241C4C722182213F9FD1D1C7B");
	}

	private static void walkRepo(Injector i, GitRepo repo) throws IOException {
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