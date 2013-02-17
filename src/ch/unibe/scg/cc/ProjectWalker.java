package ch.unibe.scg.cc;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

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

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.RealProjectFactory;
import ch.unibe.scg.cc.activerecord.RealVersionFactory;
import ch.unibe.scg.cc.activerecord.Version;

public class ProjectWalker {

	@Inject
	@Java
	Frontend javaFrontend;

	@Inject
	@Named("versions")
	HTable versions;

	@Inject
	@Named("files")
	HTable files;

	@Inject
	@Named("functions")
	HTable functions;

	@Inject
	@Named("facts")
	HTable facts;

	@Inject
	@Named("strings")
	HTable strings;

	@Inject
	RealProjectFactory projectFactory;

	@Inject
	RealVersionFactory versionFactory;

	final FileSystemManager fsManager;
	final FileSelector javaFilter = new SuffixFilter(".java");

	public ProjectWalker() {
		try {
			fsManager = VFS.getManager();
		} catch (final FileSystemException e) {
			throw new RuntimeException(e);
		}
	}

	public void crawl(String path) throws IOException {
		final File base = new File(path);
		final String[] projectNames = base.list(new SuffixFileFilter(".tar.gz"));
		for (final String projectName : projectNames) {
			FileObject project;

			project = fsManager.resolveFile(base, projectName);
			project = fsManager.resolveFile("tgz://" + project.toString().substring("file://".length()));

			crawl(project, projectName);
		}
	}

	public void crawl(FileObject project, String projectName) throws IOException {
		final FileObject[] javaFiles = project.findFiles(javaFilter);

		System.out.format("Processing files: %s%n", ArrayUtils.toString(javaFiles));
		final List<Version> versions = crawl(javaFiles);

		for (final Version version : versions) {
			final Project proj = projectFactory.create(projectName, version, "1"); // TODO
																					// get
																					// versionNumber
			javaFrontend.register(proj);
		}
	}

	List<Version> crawl(FileObject[] javaFiles) throws IOException {
		final List<Version> versions = new ArrayList<Version>();
		for (final FileObject o : javaFiles) {
			versions.add(crawl(o));
		}

		return versions;
	}

	public Version crawl(FileObject file) throws IOException {
		final InputStream is = file.getContent().getInputStream();
		final String contents = IOUtils.toString(is, "UTF-8");
		final FileName name = file.getName();
		final CodeFile codeFile = javaFrontend.register(contents, name.getBaseName());

		// XXX flushCommits get called for every single file --> bad
		// performance!?
		versions.flushCommits();
		files.flushCommits();
		functions.flushCommits();
		facts.flushCommits();
		strings.flushCommits();

		final String filePath = name.getParent().getPath() + "/" + name.getBaseName();
		final Version version = versionFactory.create(filePath, codeFile); // XXX
																			// dirty!?
		return version;
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
	public boolean traverseDescendents(FileSelectInfo fileInfo) throws Exception {
		return true;
	}
}
