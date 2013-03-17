package ch.unibe.scg.cc;

import java.util.List;

import javax.inject.Inject;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.RealCodeFileFactory;
import ch.unibe.scg.cc.activerecord.RealVersionFactory;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.lines.StringOfLines;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;
import ch.unibe.scg.cc.util.ByteUtils;

public class Frontend {

	StandardHasher standardHasher;
	ShingleHasher shingleHasher;
	protected PhaseFrontend type1;
	protected PhaseFrontend type2;
	protected StringOfLinesFactory stringOfLinesFactory;
	protected RegisterClonesBackend backend;
	protected Tokenizer tokenizer;
	RealCodeFileFactory codeFileFactory;
	RealVersionFactory versionFactory;
	Function.FunctionFactory functionFactory;

	@Inject
	Frontend(StandardHasher standardHasher, ShingleHasher shingleHasher, @Type1 PhaseFrontend type1,
			@Type2 PhaseFrontend type2, StringOfLinesFactory stringOfLinesFactory, RegisterClonesBackend backend,
			Tokenizer tokenizer, RealCodeFileFactory codeFileFactory, RealVersionFactory versionFactory,
			Function.FunctionFactory functionFactory) {
		super();
		this.standardHasher = standardHasher;
		this.shingleHasher = shingleHasher;
		this.type1 = type1;
		this.type2 = type2;
		this.stringOfLinesFactory = stringOfLinesFactory;
		this.backend = backend;
		this.tokenizer = tokenizer;
		this.codeFileFactory = codeFileFactory;
		this.versionFactory = versionFactory;
		this.functionFactory = functionFactory;
	}

	@ForTestingOnly
	public void type1Normalize(StringBuilder fileContents) {
		type1.normalize(fileContents);
	}

	@ForTestingOnly
	public void type2Normalize(StringBuilder fileContents) {
		type2.normalize(fileContents);
	}

	@ForTestingOnly
	public String type1NormalForm(CharSequence fileContents) {
		StringBuilder file = new StringBuilder(fileContents);
		type1.normalize(file);
		return file.toString();
	}

	@ForTestingOnly
	public String type2NormalForm(CharSequence fileContents) {
		StringBuilder file = new StringBuilder(fileContents);
		type1.normalize(file);
		type2.normalize(file);
		return file.toString();
	}

	public void register(Project project) {
		backend.register(project);
	}

	public void register(Version version) {
		backend.register(version);
	}

	public CodeFile register(String fileContent, String fileName) {
		CodeFile codeFile = codeFileFactory.create(fileContent);
		StringBuilder content = new StringBuilder(fileContent);
		registerWithBuilder(content, fileName, codeFile);

		backend.register(codeFile);

		return codeFile;
	}

	void registerWithBuilder(StringBuilder fileContents, String fileName, CodeFile codeFile) {
		type1.normalize(fileContents);
		List<Function> functions = tokenizer.tokenize(fileContents.toString(), fileName);
		for (Function function : functions) {
			registerFunction(codeFile, function);
		}
	}

	void registerFunction(CodeFile codeFile, Function function) {
		StringBuilder contents = new StringBuilder(function.getContents());
		StringOfLines contentsStringOfLines = stringOfLinesFactory.make(contents.toString());

		if (contentsStringOfLines.getNumberOfLines() < RegisterClonesBackend.MINIMUM_LINES) {
			return;
		}

		// type-1
		backend.registerConsecutiveLinesOfCode(contentsStringOfLines, function, Main.TYPE_1_CLONE);
		Function functionType1 = function;
		codeFile.addFunction(functionType1);

		// type-2
		type2.normalize(contents);
		String contentsString = contents.toString();
		Function functionType2 = functionFactory.makeFunction(standardHasher, functionType1.getBaseLine(),
				contentsString);
		contentsStringOfLines = stringOfLinesFactory.make(functionType2.getContents());
		backend.registerConsecutiveLinesOfCode(contentsStringOfLines, functionType2, Main.TYPE_2_CLONE);
		codeFile.addFunction(functionType2);

		// type-3
		Function functionType3 = functionFactory.makeFunction(shingleHasher, functionType2.getBaseLine(),
				contentsString);
		// drop functions which generate an empty hash
		if (functionType3.getHash().equals(ByteUtils.EMPTY_SHA1_KEY)) {
			return;
		}
		backend.shingleRegisterFunction(contentsStringOfLines, functionType3);
		codeFile.addFunction(functionType3);
	}

}
