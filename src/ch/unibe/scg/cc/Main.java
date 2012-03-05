package ch.unibe.scg.cc;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.VFS;

import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class Main {
	public static final int TYPE_1_CLONE = 1;
	public static final int TYPE_2_CLONE = 2;
	public static final int TYPE_3_CLONE = 3;


	public static void main(String[] args) throws SQLException, IOException {
		Injector injector = Guice.createInjector(new JavaModule(), new CCModule());
		ProjectWalker walker = injector.getInstance(ProjectWalker.class);
		stoppedWalk(walker);
	}

	static void stoppedWalk(ProjectWalker walker) throws SQLException, IOException {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		walk(walker);
		stopWatch.stop();
		System.out.format("Time needed: %,8d msec%n", stopWatch.getTime());
	}

	public static void walk(ProjectWalker walker) throws SQLException,
			IOException {
//		FileObject file = VFS.getManager().resolveFile(new File("."),
//				"projects/eclipse-ant.zip");
//		file = VFS.getManager()
//				.resolveFile("zip://" + file.getURL().getPath());
		FileObject file = VFS.getManager().resolveFile(new File("."),
				"projects/onefile/");
		walker.crawl(file, "eclipse-ant");
	}

}
