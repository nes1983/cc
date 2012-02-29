package ch.unibe.scg.cc;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.commons.lang3.StringUtils;

import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Location;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.javaFrontend.JavaTokenizer;
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
		registerConsecutiveLinesOfCode(stringOfLines, project, function, type);
	}
	
	public void shingleRegisterFunction(StringOfLines contentsSOL, Project project,
			Function function) {
		registerConsecutiveLinesOfCode(contentsSOL, project, function, shingleHasher, Main.TYPE_3_CLONE);
	}
	

	/**
	 * Registers a standard unit of code, typically a function, that is atomic
	 * in a sense. Lines must have at least 5 lines.
	 * 
	 * @param stringOfLines
	 * @param project
	 * @param location
	 */
	public void registerConsecutiveLinesOfCode(StringOfLines stringOfLines, Project project,
			Function function, int type) {
		registerConsecutiveLinesOfCode(stringOfLines, project, function, standardHasher, type);
	}

	void registerConsecutiveLinesOfCode(StringOfLines stringOfLines, Project project,
			Function function, Hasher hasher, int type) {
		assert stringOfLines.getNumberOfLines() >= MINIMUM_LINES;
		
		
		for (int frameStart = 0; frameStart < stringOfLines.getNumberOfLines() - MINIMUM_LINES + 1; frameStart++) {
			for (int frameLength : new int[] {MINIMUM_LINES}) {
				if (frameStart + frameLength > stringOfLines.getNumberOfLines()) {
					break;
				}
				String snippet = stringOfLines.getLines(frameStart, frameLength);
				this.registerSnippet(snippet, project, function,
						function.getBaseLine() + frameStart, frameLength,
						hasher, type);
			}
		}
	}

	private void registerSnippet(String snippet, Project project,
			Function function, int from, int length, Hasher hasher, int type) {

		byte[] hash;
		try {
			hash = hasher.hash(snippet);
		} catch(CannotBeHashedException e) {
			return;
		}
	
		registry.register(hash, project, function, from, length, type);
	}
}
