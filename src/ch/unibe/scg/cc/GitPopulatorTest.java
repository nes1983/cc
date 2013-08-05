package ch.unibe.scg.cc;

import static com.google.common.io.BaseEncoding.base16;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.inject.Qualifier;

import org.junit.Assert;
import org.junit.Test;

import ch.unibe.scg.cc.Annotations.Populator;
import ch.unibe.scg.cc.Protos.CloneType;
import ch.unibe.scg.cc.Protos.CodeFile;
import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Protos.GitRepo;
import ch.unibe.scg.cc.Protos.Project;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cc.Protos.Version;
import ch.unibe.scg.cc.javaFrontend.JavaModule;
import ch.unibe.scg.cells.CellsModule;
import ch.unibe.scg.cells.InMemoryStorage;
import ch.unibe.scg.cells.Sink;
import ch.unibe.scg.cells.Source;

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
			assertThat(projName, is("apfel"));

			fullPathString = "har://hdfs-haddock.unibe.ch/projects/dataset.har/dataset/sensei/objects/pack/pack-a33a3daca1573e82c6fbbc95846a47be4690bbe4.pack";
			projName = gitWalker.extractProjectName(fullPathString);
			assertThat(projName, is("sensei"));
		}
	}

	@Test
	public void testPopulate() throws IOException {
		Injector i = walkRepo(parseZippedGit(TESTREPO));

		Iterable<Iterable<Project>> projectPartitions = i.getInstance(
				Key.get(new TypeLiteral<Source<Project>>() {}, Populator.class));
		assertThat(Iterables.size(projectPartitions), is(1));

		Iterable<Project> projects = Iterables.getOnlyElement(projectPartitions);
		assertThat(Iterables.size(projects), is(1));

		Project p = Iterables.getOnlyElement(projects);
		assertThat(p.getName(), is("testrepo.zip"));

		Iterable<Iterable<Version>> versionPartitions = i.getInstance(
				Key.get(new TypeLiteral<Source<Version>>() {}, Populator.class));
		assertThat(Iterables.size(versionPartitions), is(1));

		Iterable<Version> versions = Iterables.getOnlyElement(versionPartitions);
		assertThat(Iterables.size(versions), is(1));
		Version v = Iterables.getOnlyElement(versions);
		assertThat(v.getName(), is("master"));
		assertThat(v.getProject(), is(p.getHash()));

		Iterable<Iterable<CodeFile>> filePartitions = i.getInstance(
				Key.get(new TypeLiteral<Source<CodeFile>>() {}, Populator.class));
		assertThat(Iterables.size(filePartitions), is(1));

		Iterable<CodeFile> files = Iterables.getOnlyElement(filePartitions);
		assertThat(Iterables.size(files), is(1));
		CodeFile cf = Iterables.getOnlyElement(files);
		assertThat(cf.getPath(), is("GitTablePopulatorTest.java"));
		assertThat(cf.getVersion(), is(v.getHash()));

		assertThat(cf.getContents().indexOf("package ch.unibe.scg.cc.mappers;"), is(0));

		Iterable<Iterable<Function>> functionPartitions = i.getInstance(
				Key.get(new TypeLiteral<Source<Function>>() {}, Populator.class));
		assertThat(Iterables.size(functionPartitions), is(1));

		Iterable<Function> functions = Iterables.getOnlyElement(functionPartitions);
		assertThat(Iterables.size(functions), is(1));

		Function fn = Iterables.getOnlyElement(functions);
		assertThat(fn.getCodeFile(), is(cf.getHash()));
		assertThat(fn.getBaseLine(), is(10));
		assertThat(fn.getContents().indexOf("public void testProjname"), is(1));

		Iterable<Iterable<Snippet>> snippetPartitions = i.getInstance(
				Key.get(new TypeLiteral<Source<Snippet>>() {}, Populator.class));
		assertThat(Iterables.size(snippetPartitions), is(1)); // means we have only one function

		Iterable<Snippet> snippets = Iterables.getOnlyElement(snippetPartitions);
		assertThat(Iterables.size(snippets), is(24));
		Snippet s0 = Iterables.get(snippets, 0);
		Snippet s1 = Iterables.get(snippets, 1);
		Snippet s7 = Iterables.get(snippets, 7);

		assertThat(s0.getFunction(), is(fn.getHash()));
		assertThat(s0.getLength(), is(5));
		assertThat(s0.getPosition(), is(0));
		assertThat(s1.getPosition(), is(1));
		assertThat(s1.getFunction(), is(fn.getHash()));
		assertThat(s7.getPosition(), is(7));
		assertThat(s7.getFunction(), is(fn.getHash()));

		// Check FunctionString
		Iterable<Iterable<Str<Function>>> functionStringPartitions = i.getInstance(
				Key.get(new TypeLiteral<Source<Str<Function>>>() {}, Populator.class));
		Iterable<Str<Function>> functionStringRow = Iterables.getOnlyElement(functionStringPartitions);
		Str<Function> functionString = Iterables.getOnlyElement(functionStringRow);
		assertThat(functionString.contents.indexOf("public void testProjnameRegex"), is(1));

		Source<Snippet> snippet2FuncsPartitions = i.getInstance(
				Key.get(new TypeLiteral<Source<Snippet>>() {}, TestSink.class));

		assertThat(Iterables.size(snippet2FuncsPartitions), is(24 - 2)); // 2 collisions

		Iterable<Snippet> snippets2Funcs = Iterables.get(snippet2FuncsPartitions, 0);
		assertThat(Iterables.size(snippets2Funcs), is(1));
	}

	@Test
	public void testPaperExampleFile2Function() throws IOException {
		Injector i = walkRepo(GitPopulatorTest.parseZippedGit("paperExample.zip"));

		Iterable<Iterable<Function>> decodedPartitions =
				i.getInstance(Key.get(new TypeLiteral<Source<Function>>() {}, Populator.class));

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
		Injector i = walkRepo(GitPopulatorTest.parseZippedGit("paperExample.zip"));
		Source<Snippet> snippet2Function = i.getInstance(Key.get(new TypeLiteral<Source<Snippet>>() {}, TestSink.class));
		assertThat(Iterables.size(snippet2Function), is(145));

		// Numbers are taken from paper. See table II.
		ByteString row03D8 = ByteString.copyFrom(new byte[] {0x03, (byte) 0xd8});
		Iterable<Snippet> partition03D8 = null;
		for(Iterable<Snippet> s2fPartition : snippet2Function) {
			if(Iterables.get(s2fPartition, 0).getHash().startsWith(row03D8)) {
				partition03D8 = s2fPartition;
				break;
			}
		}
		Assert.assertNotNull(partition03D8);
		assertThat(Iterables.size(partition03D8), is(2));

		int actualDistance = Math.abs(Iterables.get(partition03D8, 0).getPosition()
					- Iterables.get(partition03D8, 1).getPosition());
		assertThat(actualDistance, is(3));
	}

	@Test
	public void testPaperExampleFunction2Snippets() throws IOException {
		Injector i = walkRepo(GitPopulatorTest.parseZippedGit("paperExample.zip"));

		Source<Snippet> function2snippetsPartitions = i.getInstance(
				Key.get(new TypeLiteral<Source<Snippet>>() {}, Populator.class));

		// Num partitions is the number of functions. As per populator test, that's 9.
		assertThat(Iterables.size(function2snippetsPartitions), is(9));

		// We'll examine this row further in Function2RoughClones
		Iterable<Snippet> d618 = null;
		for (Iterable<Snippet> row : function2snippetsPartitions) {
			if (base16().encode(Iterables.get(row, 0).getFunction().toByteArray()).startsWith("D618")) {
				d618 = row;
				break;
			}
		}
		assert d618 != null; // Null analysis insists ...
		assertNotNull(d618);

		List<String> snippetHashes = new ArrayList<>();
		for (Snippet s : d618) {
			if (s.getCloneType() == CloneType.GAPPED) {
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

	private static Injector walkRepo(GitRepo repo) throws IOException {
		Injector i = Guice.createInjector(new CCModule(new InMemoryStorage()), new JavaModule(), new CellsModule() {
			@Override protected void configure() {
				installTable("rofl", ByteString.EMPTY, TestSink.class, new TypeLiteral<Snippet>() {},
						Snippet2FunctionsCodec.class, new InMemoryStorage());
			}
		});

		try (GitPopulator gitPopulator = i.getInstance(GitPopulator.class);
				Sink<Snippet> snippetSink = i.getInstance(Key.get(new TypeLiteral<Sink<Snippet>>() {}, TestSink.class))) {
			gitPopulator.map(repo, Arrays.asList(repo), snippetSink);
		}
		return i;
	}

	@Qualifier
	@Target({ FIELD, PARAMETER, METHOD })
	@Retention(RUNTIME)
	private static @interface TestSink {}

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