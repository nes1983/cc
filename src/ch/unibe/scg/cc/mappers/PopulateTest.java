package ch.unibe.scg.cc.mappers;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import ch.unibe.scg.cc.Frontend;
import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.ProjectFactory;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.activerecord.VersionFactory;
import ch.unibe.scg.cc.mappers.TablePopulator.TablePopulatorMapper;

@SuppressWarnings("javadoc")
public final class PopulateTest {
	@Test
	public void testFilePath() throws IOException, InterruptedException {
		@SuppressWarnings("resource")
		final Frontend javaFrontend = mock(Frontend.class);

		@SuppressWarnings("resource")
		final HTable projects = mock(HTable.class), versions = mock(HTable.class), codefiles = mock(HTable.class),
				functions = mock(HTable.class), strings = mock(HTable.class), hashfactContent = mock(HTable.class);
		final ProjectFactory projectFactory = mock(ProjectFactory.class);
		final VersionFactory versionFactory = mock(VersionFactory.class);
		final CharsetDetector charsetDetector = mock(CharsetDetector.class);
		final Version version = mock(Version.class);
		final Project project = mock(Project.class);

		when(charsetDetector.charsetOf(new byte[] { 0x20 })).thenReturn("UTF-8");
		when(versionFactory.create(eq("postgresql/simon/myfile.java"), any(CodeFile.class))).thenReturn(version);
		when(projectFactory.create(eq("postgresql"), eq(version), any(String.class))).thenReturn(project);

		TablePopulatorMapper mapper = new TablePopulatorMapper(javaFrontend, projects, versions, codefiles,
				functions, strings, hashfactContent, projectFactory, versionFactory, charsetDetector);

		mapper.map(new Text("postgresql/simon/myfile.java"), new BytesWritable(new byte[] { 0x20 }), null);

		verify(javaFrontend).register(eq(" "));
		verify(javaFrontend).register(eq(version));
		verify(javaFrontend).register(eq(project));
	}
}