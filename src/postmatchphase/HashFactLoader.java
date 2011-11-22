package postmatchphase;

import java.io.IOException;
import java.io.InputStream;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;

import ch.unibe.scg.cc.activerecord.HashFact;
import ch.unibe.scg.cc.lines.ModifiableLinesFactory;
import ch.unibe.scg.cc.lines.StringOfLines;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;

public class HashFactLoader {

	@Inject
	StringOfLinesFactory stringOfLinesFactory;
	
	@Inject
	FileObject projects;

	String myLoadFile(HashFact fact) throws IOException {
		FileObject file = projects.getChild(fact.getProject().getName());
		InputStream 	is = file.getContent().getInputStream();
		String contents = IOUtils.toString(is, "UTF-8");
		return contents;
	}
	
	public StringOfLines loadFile(HashFact fact) throws IOException {
		return stringOfLinesFactory.make(myLoadFile(fact));
	}
}
