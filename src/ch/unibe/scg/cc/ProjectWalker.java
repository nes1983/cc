package ch.unibe.scg.cc;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
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
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.activerecord.RealVersionFactory;

public class ProjectWalker {

	@Inject
	@Java
	Frontend javaFrontend;

	@Inject @Named("projects")
	HTable projects;
	
	@Inject @Named("versions")
	HTable versions;
	
	@Inject @Named("files")
	HTable codefiles;
	
	@Inject @Named("functions")
	HTable functions;
	
	@Inject @Named("strings")
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

	public void crawl(FileObject project, String projectName) throws SQLException, IOException {
		FileObject[] javaFiles = project.findFiles(javaFilter);

		System.out.format("Processing files: %s%n", ArrayUtils.toString(javaFiles));
		List<Version> versions = crawl(javaFiles);
		
		for(Version version : versions) {
			Project proj = projectFactory.create(projectName, version, 1); // TODO get versionNumber
			javaFrontend.register(proj);
		}
	}

	List<Version> crawl(FileObject[] javaFiles) throws SQLException, IOException {
		List<Version> versions = new ArrayList<Version>();
		for (FileObject o : javaFiles) {
			versions.add(crawl(o));
		}
		
		return versions;
	}

	public Version crawl(FileObject file) throws IOException {
		InputStream 	is = file.getContent().getInputStream();
		String contents = IOUtils.toString(is, "UTF-8");
		FileName name = file.getName();
		CodeFile codeFile = javaFrontend.register(contents, name.getBaseName());
		
		// XXX flushCommits get called for every single file --> bad performance!?
        projects.flushCommits();
        versions.flushCommits();
        codefiles.flushCommits();
        functions.flushCommits();
        strings.flushCommits();
        
        String filePath = name.getParent().getPath() + "/" + name.getBaseName();
		Version version = versionFactory.create(filePath, codeFile); // XXX dirty!?
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
	public boolean traverseDescendents(FileSelectInfo fileInfo)
			throws Exception {
		return true;
	}
}
