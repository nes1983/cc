package ch.unibe.scg.cc.javaFrontend;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.inject.Inject;

import ch.unibe.scg.cc.StandardHasher;
import ch.unibe.scg.cc.Tokenizer;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Function.FunctionFactory;
import dk.brics.automaton.AutomatonMatcher;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;

public class JavaTokenizer implements Tokenizer {
	// TODO: the last function in a class always catches one closing curly
	// bracket ("}") too much
	final String splitterRegex = "([a-zA-Z \\[\\]<>,]*\\([a-zA-Z \\[\\]<>,]*\\)[a-zA-Z \\[\\]<>,]*\\{|([^\n]*[^.]|\\n)(class|interface)[^\n]*)[^\n]*";
	final Pattern wrongMethodKeywords = Pattern.compile("\\b(switch|while|if|for)\\b\\s*\\(");

	@Inject
	FunctionFactory functionFactory;

	@Inject
	StandardHasher standardHasher;

	final RunAutomaton splitter;

	public JavaTokenizer() {
		RegExp splitterRegexp = new RegExp(splitterRegex);
		splitter = new RunAutomaton(splitterRegexp.toAutomaton());
	}

	@Override
	public List<Function> tokenize(String file, String fileName) {
		int currentLineNumber = 0;
		int lineLength = 0;
		int lastStart = 0;

		List<Function> ret = new ArrayList<Function>();
		AutomatonMatcher m = splitter.newMatcher(file);

		while (m.find()) {
			int start = m.start();

			// TODO: hack - can be thrown away with the new regex-engine
			// ensure that start is at the beginning of a line
			if (start > 0 && file.charAt(start - 1) != '\n') {
				start = file.substring(0, start).lastIndexOf('\n') + 1;
			}

			// enlarge match if regex captures a "wrong" method
			if (wrongMethodKeywords.matcher(m.group()).find()) {
				continue;
			}

			String currentFunctionString = file.substring(lastStart, start);
			lineLength = countOccurrences(currentFunctionString, '\n');

			// don't save first match (package & import statements)
			if (currentLineNumber > 0) {
				Function function = functionFactory.makeFunction(standardHasher, currentLineNumber,
						currentFunctionString);
				ret.add(function);
			}

			lastStart = start;
			currentLineNumber += lineLength;
		}
		String currentFunctionString = file.substring(lastStart, file.length());
		Function function = functionFactory.makeFunction(standardHasher, currentLineNumber, currentFunctionString);
		ret.add(function);
		return ret;
	}

	public static int countOccurrences(String haystack, char needle) {

		int count = 0;
		for (int i = 0; i < haystack.length(); i++) {
			if (haystack.charAt(i) == needle) {
				count++;
			}
		}
		return count;
	}
}
