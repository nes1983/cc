package ch.unibe.scg.cc;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import javax.inject.Named;

import org.apache.hadoop.mapreduce.Counter;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.lines.StringOfLines;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;
import ch.unibe.scg.cc.mappers.GuiceResource;

import com.google.inject.Inject;

public class RegisterClonesBackend implements Closeable {
	static Logger logger = Logger.getLogger(RegisterClonesBackend.class.getName());
	public static final int MINIMUM_LINES = 5;
	public static final int MINIMUM_FRAME_SIZE = MINIMUM_LINES;
	Registry registry;
	StandardHasher standardHasher;
	ShingleHasher shingleHasher;
	StringOfLinesFactory stringOfLinesFactory;

	// Optional because in MRMain, we have an injector that does not set this
	// property, and can't, because it doesn't have the counter available.
	@Inject(optional = true)
	@Named(GuiceResource.COUNTER_CANNOT_BE_HASHED)
	Counter cannotBeHashedCounter;
	@Inject(optional = true)
	@Named(GuiceResource.COUNTER_SUCCESSFULLY_HASHED)
	Counter successfullyHashedCounter;

	@Inject
	public RegisterClonesBackend(Registry registry, StandardHasher standardHasher, ShingleHasher shingleHasher,
			StringOfLinesFactory stringOfLinesFactory) {
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
	public void registerFunction(String lines, Project project, Function function, byte type) {
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
	public void registerConsecutiveLinesOfCode(StringOfLines stringOfLines, Function function, byte type) {
		registerConsecutiveLinesOfCode(stringOfLines, function, standardHasher, type);
	}

	void registerConsecutiveLinesOfCode(StringOfLines stringOfLines, Function function, Hasher hasher, byte type) {
		assert stringOfLines.getNumberOfLines() >= MINIMUM_LINES;

		for (int frameStart = 0; frameStart + MINIMUM_LINES < stringOfLines.getNumberOfLines(); frameStart++) {
			String snippet = stringOfLines.getLines(frameStart, MINIMUM_LINES);
			this.registerSnippet(snippet, function, frameStart, MINIMUM_LINES, hasher, type);
		}
	}

	private void registerSnippet(String snippet, Function function, int from, int length, Hasher hasher, byte type) {
		byte[] hash;
		try {
			hash = hasher.hash(snippet);
			successfullyHashedCounter.increment(1);
		} catch (CannotBeHashedException e) {
			cannotBeHashedCounter.increment(1);
			return;
		}
		registry.register(hash, snippet, function, from, length, type);
	}

	public void register(CodeFile codeFile) {
		registry.register(codeFile);
		List<Function> functions = codeFile.getFunctions();
		for (Function function : functions) {
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

	@Override
	public void close() throws IOException {
		registry.close();
	}
}
