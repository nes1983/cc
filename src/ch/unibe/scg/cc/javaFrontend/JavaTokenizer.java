package ch.unibe.scg.cc.javaFrontend;

import java.util.ArrayList;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.apache.commons.lang3.StringUtils;


import ch.unibe.scg.cc.Tokenizer;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Location;
import dk.brics.automaton.AutomatonMatcher;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;

@Singleton
public class JavaTokenizer implements Tokenizer {
	
	
	final String splitterRegex = "([a-zA-Z \\[\\]<>,]*\\([a-zA-Z \\[\\]<>,]*\\)[a-zA-Z \\[\\]<>,]*\\{|([^\n]*[^.]|\\n)(class|interface)[^\n]*)[^\n]*";

	@Inject
	Provider<Function> functionProvider;
	
	final RunAutomaton splitter;
	
	public JavaTokenizer() {
		 RegExp splitterRegexp = new RegExp(splitterRegex);
		 splitter = new RunAutomaton( splitterRegexp.toAutomaton());
	}
	
	/* (non-Javadoc)
	 * @see ch.unibe.scg.cc.javaFrontend.Tokenizer#tokenize(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public List<Function> tokenize(String file, String fileName, String filePath) {
		int currentLineNumber = 0;
		int lineLength=0;
		int lastStart = 0;

		List<Function> ret = new ArrayList<Function>();
		AutomatonMatcher m = splitter.newMatcher(file);

		while(m.find()) {
			String currentFunctionString = file.substring(lastStart,m.start());
			lineLength = countOccurrences(currentFunctionString, '\n');
			Function function = makeFunction(currentLineNumber, fileName, filePath, currentFunctionString);
			ret.add(function);
			
			lastStart = m.start();
			currentLineNumber += lineLength;
		} 
		String currentFunctionString = file.substring(lastStart, file.length());
		//Location location = makeLocation(currentLineNumber, countOccurrences(currentFunction, '\n'));
		Function function = makeFunction(currentLineNumber, fileName, filePath, currentFunctionString);
		ret.add(function);
		return ret;
	}
	
	Function makeFunction(int baseLine, String fname, String filePath, String contents) {
		Function f = functionProvider.get();
		f.setBaseLine(baseLine);
		f.setFname(fname);
		f.setFile_path(filePath);
		f.setContents(contents);
		return f;
	}
	
	public static int countOccurrences(String haystack, char needle)	{

	    int count = 0;
	    for (int i=0; i < haystack.length(); i++) {
	        if (haystack.charAt(i) == needle) {
	             count++;
	        }
	    }
	    return count;
	}
}
