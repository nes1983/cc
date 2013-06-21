package ch.unibe.scg.cc.mappers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import junit.framework.Assert;

import org.apache.hadoop.mapreduce.Counter;
import org.junit.Test;
import org.mockito.Mockito;

import ch.unibe.scg.cc.Frontend;
import ch.unibe.scg.cc.StandardHasher;
import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.ProjectFactory;
import ch.unibe.scg.cc.activerecord.VersionFactory;
import ch.unibe.scg.cc.mappers.GitTablePopulator.GitTablePopulatorMapper;
import ch.unibe.scg.cc.mappers.TablePopulator.CharsetDetector;
import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;
import com.google.inject.Injector;

public final class GitTablePopulatorTest {
	private static final String TESTREPO = "testrepo.zip";

	@Test
	public void testProjnameRegex() {
		GitTablePopulatorMapper gtpm = new GitTablePopulatorMapper(null, null, null, null, null, null, null, null,
				null);
		String fullPathString = "har://hdfs-haddock.unibe.ch/projects/testdata.har"
				+ "/apfel/.git/objects/pack/pack-b017c4f4e226868d8ccf4782b53dd56b5187738f.pack";
		String projName = gtpm.getProjName(fullPathString);
		Assert.assertEquals("apfel", projName);

		fullPathString = "har://hdfs-haddock.unibe.ch/projects/dataset.har/dataset/sensei/objects/pack/pack-a33a3daca1573e82c6fbbc95846a47be4690bbe4.pack";
		projName = gtpm.getProjName(fullPathString);
		Assert.assertEquals("sensei", projName);
	}

	@Test
	public void testPopulate() throws IOException {
		Injector i = Guice.createInjector(new CCModule(), new JavaModule());

		Frontend javaFrontend = mock(Frontend.class);
		when(javaFrontend.register(Mockito.any(String.class), Mockito.any(String.class)))
				.thenReturn(new CodeFile(i.getInstance(StandardHasher.class), ""));

		GitTablePopulatorMapper mapper = new GitTablePopulatorMapper(javaFrontend,
				null, null, null, null, null, // HTables.
				i.getInstance(ProjectFactory.class), i.getInstance(VersionFactory.class),
				i.getInstance(CharsetDetector.class));
		mapper.processedFilesCounter = mock(Counter.class);

		ZipInputStream packFile = new ZipInputStream(GitTablePopulatorTest.class.getResourceAsStream(TESTREPO));
		for (ZipEntry entry; (entry = packFile.getNextEntry()) != null;) {
			if (entry.getName().endsWith(".pack")) {
				break;
			}
		}

		ZipInputStream packedRefs = new ZipInputStream(GitTablePopulatorTest.class.getResourceAsStream(TESTREPO));
		for (ZipEntry entry; (entry = packedRefs.getNextEntry()) != null; ) {
			if (entry.getName().endsWith("packed-refs")) {
				break;
			}
		}

		mapper.mapRepo(packedRefs, packFile, "Captain Hook");
	}
}