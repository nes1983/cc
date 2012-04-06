package ch.unibe.scg.cc;

import java.util.List;

import javax.inject.Inject;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.RealCodeFileFactory;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.RealVersionFactory;
import ch.unibe.scg.cc.lines.StringOfLines;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;

public  class Frontend {
	
	@Inject
	StandardHasher standardHasher;

	@Inject
	ShingleHasher shingleHasher;

	
	@Inject @Type1
	protected PhaseFrontent type1;
	
	@Inject @Type2
	protected PhaseFrontent type2;
	
	@Inject
	protected StringOfLinesFactory stringOfLinesFactory;
	
	@Inject 
	protected RegisterClonesBackend backend;
	
	@Inject
	protected Tokenizer tokenizer;

	@Inject
	RealCodeFileFactory codeFileFactory;
	
	@Inject
	RealVersionFactory versionFactory;
	
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
	
	void register(Project project) {
		backend.register(project);
	}

	CodeFile register(String fileContents, String fileName) {
		CodeFile codeFile = codeFileFactory.create(fileContents);
		
		boolean codeFileAlreadyInDB = backend.lookup(codeFile);
		if(codeFileAlreadyInDB) {
			System.out.println("file is already in DB: " + codeFile.getFileContentsHash());
			return codeFile; // already processed this file =)
		}
		
		StringBuilder contents = new StringBuilder(fileContents);
		registerWithBuilder(contents, fileName, codeFile);
		
		backend.register(codeFile);
		
		return codeFile;
	}

	void registerWithBuilder(StringBuilder fileContents, String fileName, CodeFile codeFile) {
		type1.normalize(fileContents);
		List<Function> functions = tokenizer.tokenize(fileContents.toString(), fileName);
		for(Function function : functions) {
			codeFile.addFunction(function);
			registerFunction(function);
		}	
	}

	void registerFunction(Function function) {
		StringBuilder contents = function.getContents();
		StringOfLines contentsStringOfLines = asSOL(contents);
		
		if(contentsStringOfLines.getNumberOfLines() < backend.MINIMUM_LINES) {
			return;
		}
		
		backend.registerConsecutiveLinesOfCode(contentsStringOfLines, function, Main.TYPE_1_CLONE);
		type2.normalize(contents);
		contentsStringOfLines = asSOL(contents);
		backend.registerConsecutiveLinesOfCode(contentsStringOfLines, function, Main.TYPE_2_CLONE);
		backend.shingleRegisterFunction(contentsStringOfLines, function);
	}
	
	private StringOfLines asSOL(StringBuilder sb) {
		return stringOfLinesFactory.make(sb.toString());
	}
	
}
