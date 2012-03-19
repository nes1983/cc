package ch.unibe.scg.cc;

import java.util.List;

import javax.inject.Inject;
import javax.inject.Provider;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Project;
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
	Provider<CodeFile> codeFileProvider;
	
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
	

	public void register(String fileContents, Project project, String fileName, String filePath) {
		project.setFilePath(filePath + "/" + fileName); // XXX dirty!?
		
		CodeFile codeFile = codeFileProvider.get();
		codeFile.setProject(project);
		codeFile.setFileContents(fileContents);
		
		// register project
		backend.register(project);
		
		boolean codeFileAlreadyInDB = backend.register(codeFile);
		if(codeFileAlreadyInDB) {
			System.out.println("file is already in DB: " + codeFile.getProject().getFilePath());
			return; // already processed this file =)
		}
		
		StringBuilder contents = new StringBuilder(fileContents);
		registerWithBuilder(contents, project, fileName, codeFile);
	}

	void registerWithBuilder(StringBuilder fileContents, Project project, String fileName, CodeFile codeFile) {
		type1.normalize(fileContents);
		List<Function> functions = tokenizer.tokenize(fileContents.toString(), fileName, codeFile);
		for(Function function : functions) {
			registerFunction(project, function);
		}	
	}

	void registerFunction(Project project, Function function) {
		StringBuilder contents = function.getContents();
		StringOfLines contentsStringOfLines = asSOL(contents);
		
		if(contentsStringOfLines.getNumberOfLines() < backend.MINIMUM_LINES) {
			return;
		}
		
		backend.registerConsecutiveLinesOfCode(contentsStringOfLines, project, function, Main.TYPE_1_CLONE);
		type2.normalize(contents);
		contentsStringOfLines = asSOL(contents);
		backend.registerConsecutiveLinesOfCode(contentsStringOfLines, project, function, Main.TYPE_2_CLONE);
		backend.shingleRegisterFunction(contentsStringOfLines, project, function);
	}
	
	private StringOfLines asSOL(StringBuilder sb) {
		return stringOfLinesFactory.make(sb.toString());
	}
	
}
