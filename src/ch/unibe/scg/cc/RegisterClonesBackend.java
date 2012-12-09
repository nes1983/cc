package ch.unibe.scg.cc;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.RealProject;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.lines.StringOfLines;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;

public class RegisterClonesBackend {

	public final int MINIMUM_LINES = 10;
	public final int MINIMUM_FRAME_SIZE = MINIMUM_LINES;
	final byte[] emptySHA1Key = new byte[] {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
	CloneRegistry registry;
	StandardHasher standardHasher;
	ShingleHasher shingleHasher;
	StringOfLinesFactory stringOfLinesFactory;

	@Inject
	public RegisterClonesBackend(CloneRegistry registry,
			StandardHasher standardHasher, ShingleHasher shingleHasher,
			StringOfLinesFactory stringOfLinesFactory) {
		super();
		this.registry = registry;
		this.standardHasher = standardHasher;
		this.shingleHasher = shingleHasher;
		this.stringOfLinesFactory = stringOfLinesFactory;
	}


	
	/**
	 * Registers a standard unit of code, typically a function, that is atomic
	 * in a sense. Lines must have at least 5 lines.
	 * 
	 * @param lines
	 * @param project
	 * @param location
	 */
	public void registerFunction(String lines, RealProject project,
			Function function, byte type) {
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
			Function function, byte type) {
		registerConsecutiveLinesOfCode(stringOfLines, function, standardHasher, type);
	}

	void registerConsecutiveLinesOfCode(StringOfLines stringOfLines, 
			Function function, Hasher hasher, byte type) {
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

	private void registerSnippet(String snippet, Function function, int from, int length, Hasher hasher, byte type) {

		byte[] hash;
		try {
			hash = hasher.hash(snippet);
			
			if(Arrays.equals(hash, emptySHA1Key)) //can happen with the shingle hasher
				return;
		} catch(CannotBeHashedException e) {
			return;
		}
		registry.register(hash, snippet, function, from, length, type);
	}

	public void register(CodeFile codeFile) {
		registry.register(codeFile);
		List<Function> functions = codeFile.getFunctions();
		functions = functions.subList(1, functions.size()); //XXX removes first function with the whole filecontent
		for(Function function : functions) {
			register(function);
		}
	}

	private void register(Function function) {
		registry.register(function);
	}

	public void register(Project project) {
		registry.register(project);
	}

	public void register(Version version) {
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
	
	/**
	 * looks up the version
	 * @param version
	 * @return returns true, if version is already in the database, otherwise false.
	 */
	public boolean lookup(Version version) {
		byte[] hash = version.getHash();
		return registry.lookupVersion(hash);
	}
	
	/**
	 * looks up the project
	 * @param project
	 * @return returns true, if project is already in the database, otherwise false.
	 */
	public boolean lookup(Project project) {
		byte[] hash = project.getHash();
		return registry.lookupProject(hash);
	}
}
