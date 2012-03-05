package ch.unibe.scg.cc;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.apache.hadoop.hbase.client.HTable;

import ch.unibe.scg.cc.activerecord.Project;

public class ProjectWalker {

	@Inject
	@Java
	Frontend javaFrontend;
	
	@Inject
	HTable htable;

	@Inject
	Provider<Project> projectProvider;

	final FileSystemManager fsManager;
	final FileSelector javaFilter = new SuffixFilter(".java");

	public ProjectWalker() {
		try {
			fsManager = VFS.getManager();
		} catch (FileSystemException e) {
			throw new RuntimeException(e);
		}
	}

	public void crawl(String path) throws SQLException, IOException {
		SuffixFileFilter filter = new SuffixFileFilter(".tar.gz");
		File base = new File(path);
		String[] projectNames = base
				.list((FilenameFilter) new SuffixFileFilter(".tar.gz"));
		for (String projectName : projectNames) {
			FileObject project;

			project = fsManager.resolveFile(base, projectName);
			project = fsManager.resolveFile("tgz://"
					+ project.toString().substring("file://".length()));

			crawl(project, projectName);
		}
	}

	public void crawl(FileObject project) throws SQLException, IOException {
		crawl(project, project.getName().getBaseName());
	}

	public void crawl(FileObject project, String projectName) throws SQLException, IOException {
	
		Project currentProject = makeProject(projectName);
	
		FileObject[] javaFiles = project.findFiles(javaFilter);

		System.out.format("Processing files: %s%n", ArrayUtils.toString(javaFiles));
		crawl(javaFiles, currentProject);
	}

	Project makeProject(String projectName) {
		Project project = projectProvider.get();
		project.setName(projectName);
		return project;
	}

	void crawl(FileObject[] javaFiles, Project project) throws SQLException, IOException {
		for (FileObject o : javaFiles) {
			crawl(o, project);
		}
	}

	public void crawl(FileObject file, Project project) throws IOException {
		InputStream 	is = file.getContent().getInputStream();
		String contents = IOUtils.toString(is, "UTF-8");
		FileName name = file.getName();
		javaFrontend.register(contents, project, name.getBaseName(), name
				.getParent().getPath());
        htable.flushCommits();
	}
}

class SuffixFilter implements FileSelector {

	final String suffix;

	public SuffixFilter(String suffix) {
		this.suffix = suffix;
	}

	@Override
	public boolean includeFile(FileSelectInfo fileInfo) throws Exception {
		return fileInfo.getFile().getName().toString().endsWith(suffix);
	}

	@Override
	public boolean traverseDescendents(FileSelectInfo fileInfo)
			throws Exception {
		return true;
	}
}
