package ch.unibe.scg.cc;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

import javax.inject.Inject;

import ch.unibe.scg.cc.Tokenizer.SnippetWithBaseline;
import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.CodeFileFactory;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.activerecord.VersionFactory;
import ch.unibe.scg.cc.lines.StringOfLines;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;

public class Frontend implements Closeable {
	StandardHasher standardHasher;
	ShingleHasher shingleHasher;
	protected PhaseFrontend type1;
	protected PhaseFrontend type2;
	protected StringOfLinesFactory stringOfLinesFactory;
	protected Backend backend;
	protected Tokenizer tokenizer;
	CodeFileFactory codeFileFactory;
	VersionFactory versionFactory;
	Function.FunctionFactory functionFactory;

	@Inject
	Frontend(StandardHasher standardHasher, ShingleHasher shingleHasher, @Type1 PhaseFrontend type1,
			@Type2 PhaseFrontend type2, StringOfLinesFactory stringOfLinesFactory, Backend backend,
			Tokenizer tokenizer, CodeFileFactory codeFileFactory, VersionFactory versionFactory,
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

	public CodeFile register(String fileContent) {
		CodeFile codeFile = codeFileFactory.create(fileContent);
		StringBuilder content = new StringBuilder(fileContent);
		registerWithBuilder(content, codeFile);

		backend.register(codeFile);

		return codeFile;
	}

	void registerWithBuilder(StringBuilder fileContents, CodeFile codeFile) {
		for (SnippetWithBaseline function : tokenizer.tokenize(fileContents.toString())) {
			registerFunction(codeFile, function);
		}
	}

	void registerFunction(CodeFile codeFile, SnippetWithBaseline function) {
		// type-1
		StringBuilder normalized = new StringBuilder(function.getSnippet());
		type1.normalize(normalized);
		StringOfLines normalizedSOL = stringOfLinesFactory.make(normalized.toString());
		if (normalizedSOL.getNumberOfLines() < Backend.MINIMUM_LINES) {
			return;
		}

		Function functionType1 = functionFactory.makeFunction(
				standardHasher, function.getBaseLine(), normalized.toString(), function.getSnippet());
		backend.registerConsecutiveLinesOfCode(normalizedSOL, functionType1, Main.TYPE_1_CLONE);
		codeFile.addFunction(functionType1);

		// type-2
		type2.normalize(normalized);
		Function functionType2 = functionFactory.makeFunction(shingleHasher, function.getBaseLine(),
				normalized.toString(), function.getSnippet());
		normalizedSOL = stringOfLinesFactory.make(normalized.toString());
		backend.registerConsecutiveLinesOfCode(normalizedSOL, functionType2, Main.TYPE_2_CLONE);
		codeFile.addFunction(functionType2);

		// type-3
		Function functionType3 = functionFactory.makeFunction(
				shingleHasher, functionType2.getBaseLine(),	normalized.toString(), function.getSnippet());
		assert !Arrays.equals(functionType3.getHash(), ByteUtils.EMPTY_SHA1_KEY);
		backend.shingleRegisterFunction(normalizedSOL, functionType3);
		codeFile.addFunction(functionType3);
	}


	@Override
	public void close() throws IOException {
		backend.close();
	}
}
