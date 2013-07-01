package ch.unibe.scg.cc.mappers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.mapreduce.Counter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import ch.unibe.scg.cc.CCModule;
import ch.unibe.scg.cc.Frontend;
import ch.unibe.scg.cc.StandardHasher;
import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.ProjectFactory;
import ch.unibe.scg.cc.activerecord.VersionFactory;
import ch.unibe.scg.cc.javaFrontend.JavaModule;
import ch.unibe.scg.cc.mappers.GitTablePopulator.GitTablePopulatorMapper;

import com.google.inject.Guice;
import com.google.inject.Injector;

@SuppressWarnings("javadoc")
public final class GitTablePopulatorTest {
	private static final String TESTREPO = "testrepo.zip";

	@Test
	public void testProjnameRegex() {
		GitTablePopulatorMapper gtpm = new GitTablePopulatorMapper(null, null, null, null);
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

		@SuppressWarnings("resource") // Mocked frontend shouldn't be closed.
		Frontend javaFrontend = mock(Frontend.class);
		when(javaFrontend.register(Mockito.any(String.class)))
				.thenReturn(new CodeFile(i.getInstance(StandardHasher.class), ""));

		GitTablePopulatorMapper mapper = new GitTablePopulatorMapper(javaFrontend,
				i.getInstance(ProjectFactory.class), i.getInstance(VersionFactory.class),
				i.getInstance(CharsetDetector.class));
		mapper.processedFilesCounter = mock(Counter.class);

		try (ZipInputStream packFile = new ZipInputStream(GitTablePopulatorTest.class.getResourceAsStream(TESTREPO));
				ZipInputStream packedRefs = new ZipInputStream(
						GitTablePopulatorTest.class.getResourceAsStream(TESTREPO))) {
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

			mapper.mapRepo(packedRefs, packFile, "Captain Hook");
		}
	}
}