package ch.unibe.scg.cc;

import java.util.List;

import javax.inject.Inject;

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
		StringBuilder contents = new StringBuilder(fileContents);
		registerWithBuilder(contents, project, fileName, filePath);
	}
	
	void registerWithBuilder(StringBuilder fileContents, Project project, String fileName, String filePath) {
		type1.normalize(fileContents);
		List<Function> functions = tokenizer.tokenize(fileContents.toString(), fileName, filePath);
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
