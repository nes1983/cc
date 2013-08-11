package ch.unibe.scg.cc.javaFrontend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

import ch.unibe.scg.cc.Protos.Function;
import ch.unibe.scg.cc.Tokenizer;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;
import dk.brics.automaton.AutomatonMatcher;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;

class JavaTokenizer implements Tokenizer {
	final private static long serialVersionUID = 1L;

	// TODO: the last function in a class always catches one closing curly
	// bracket ("}") too much
	final static String splitterRegex =
			"([a-zA-Z \\[\\]<>,]*\\([a-zA-Z \\[\\]<>,]*\\)[a-zA-Z \\[\\]<>,]*\\{|([^\n]*[^.]|\\n)(class|interface)[^\n]*)[^\n]*";

	final private Pattern wrongMethodKeywords = Pattern.compile("\\b(switch|while|if|for)\\b\\s*\\(");
	final private RunAutomaton splitter = new RunAutomaton(new RegExp(splitterRegex).toAutomaton());

	// Prevent subclassing.
	JavaTokenizer() {}

	@Override
	public Iterable<Function> tokenize(String file) {
		int currentLineNumber = 0;
		int lastStart = 0;

		Collection<Function> ret = new ArrayList<>();
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
			// don't save first match (package & import statements)
			if (currentLineNumber > 0) {
				ret.add(Function.newBuilder()
						.setBaseLine(currentLineNumber).setContents(currentFunctionString).build());
			}

			lastStart = start;
			currentLineNumber += StringOfLinesFactory.countOccurrences(currentFunctionString, '\n');
		}
		String currentFunctionString = file.substring(lastStart, file.length());
		ret.add(Function.newBuilder()
				.setBaseLine(currentLineNumber).setContents(currentFunctionString).build());
		return ret;
	}
}
