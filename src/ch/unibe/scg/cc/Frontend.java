package ch.unibe.scg.cc;

import java.util.List;

import javax.inject.Inject;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.RealCodeFileFactory;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.RealVersionFactory;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.lines.StringOfLines;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;

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
	
	@Inject
	Frontend(StandardHasher standardHasher, ShingleHasher shingleHasher,
			@Type1 PhaseFrontend type1, @Type2 PhaseFrontend type2,
			StringOfLinesFactory stringOfLinesFactory,
			RegisterClonesBackend backend, Tokenizer tokenizer,
			RealCodeFileFactory codeFileFactory,
			RealVersionFactory versionFactory) {
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
		boolean alreadyInDB = backend.lookup(project);
		if(alreadyInDB) { return; }
		
		backend.register(project);
	}
	
	public void register(Version version) {
		boolean alreadyInDB = backend.lookup(version);
		if(alreadyInDB) { return; }
		
		backend.register(version);
	}

	public CodeFile register(String fileContent, String fileName) {
		CodeFile codeFile = codeFileFactory.create(fileContent);
		
		boolean alreadyInDB = backend.lookup(codeFile);
		if(alreadyInDB) {
			return codeFile; // already processed this file =)
		}
		
		StringBuilder content = new StringBuilder(fileContent);
		registerWithBuilder(content, fileName, codeFile);
		
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
