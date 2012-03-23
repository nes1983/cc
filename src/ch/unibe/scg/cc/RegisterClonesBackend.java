package ch.unibe.scg.cc;

import javax.inject.Inject;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.lines.StringOfLines;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;

public class RegisterClonesBackend {

	public final int MINIMUM_LINES = 5;
	public final int MINIMUM_FRAME_SIZE = MINIMUM_LINES;


	@Inject
	CloneRegistry registry;

	@Inject
	StandardHasher standardHasher;

	@Inject
	ShingleHasher shingleHasher;

	@Inject
	StringOfLinesFactory stringOfLinesFactory;
	
	/**
	 * Registers a standard unit of code, typically a function, that is atomic
	 * in a sense. Lines must have at least 5 lines.
	 * 
	 * @param lines
	 * @param project
	 * @param location
	 */
	public void registerFunction(String lines, Project project,
			Function function, int type) {
		StringOfLines stringOfLines = stringOfLinesFactory.make(lines);
		registerConsecutiveLinesOfCode(stringOfLines, function, type);
	}
	
	public void shingleRegisterFunction(StringOfLines contentsSOL, Function function) {
		registerConsecutiveLinesOfCode(contentsSOL, function, shingleHasher, Main.TYPE_3_CLONE);
	}
	

	/**
	 * Registers a standard unit of code, typically a function, that is atomic
	 * in a sense. Lines must have at least 5 lines.
	 * 
	 * @param stringOfLines
	 * @param project
	 * @param location
	 */
	public void registerConsecutiveLinesOfCode(StringOfLines stringOfLines, 
			Function function, int type) {
		registerConsecutiveLinesOfCode(stringOfLines, function, standardHasher, type);
	}

	void registerConsecutiveLinesOfCode(StringOfLines stringOfLines, 
			Function function, Hasher hasher, int type) {
		assert stringOfLines.getNumberOfLines() >= MINIMUM_LINES;
		
		
		for (int frameStart = 0; frameStart < stringOfLines.getNumberOfLines() - MINIMUM_LINES + 1; frameStart++) {
			if (frameStart + MINIMUM_LINES > stringOfLines.getNumberOfLines()) {
				break;
			}
			String snippet = stringOfLines.getLines(frameStart, MINIMUM_LINES);
			this.registerSnippet(snippet, function,
					function.getBaseLine() + frameStart, MINIMUM_LINES,
					hasher, type);
		}
	}

	private void registerSnippet(String snippet, Function function, int from, int length, Hasher hasher, int type) {

		byte[] hash;
		try {
			hash = hasher.hash(snippet);
		} catch(CannotBeHashedException e) {
			return;
		}
		registry.register(hash, function, from, length, type);
	}

	public void register(CodeFile codeFile) {
		registry.register(codeFile);
		for(Function function : codeFile.getFunctions())
			register(function);
	}

	private void register(Function function) {
		registry.register(function);
	}

	public void register(Project project) {
		registry.register(project);
		register(project.getVersion());
	}

	private void register(Version version) {
		registry.register(version);
	}
	
	/**
	 * looks up the codefile
	 * @param codeFile
	 * @return returns true, if file is already in the database, otherwise false.
	 */
	public boolean lookup(CodeFile codeFile) {
		byte[] hash = codeFile.getFileContentsHash();
		return registry.lookupCodeFile(hash);
	}
}
