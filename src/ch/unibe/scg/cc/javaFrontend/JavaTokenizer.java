package ch.unibe.scg.cc.javaFrontend;

import java.util.Collection;
import java.util.regex.Pattern;

import ch.unibe.scg.cc.Tokenizer;

import com.google.common.collect.Lists;

import dk.brics.automaton.AutomatonMatcher;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;

public class JavaTokenizer implements Tokenizer {
	// TODO: the last function in a class always catches one closing curly
	// bracket ("}") too much
	final String splitterRegex =
			"([a-zA-Z \\[\\]<>,]*\\([a-zA-Z \\[\\]<>,]*\\)[a-zA-Z \\[\\]<>,]*\\{|([^\n]*[^.]|\\n)(class|interface)[^\n]*)[^\n]*";
	final Pattern wrongMethodKeywords = Pattern.compile("\\b(switch|while|if|for)\\b\\s*\\(");

	final RunAutomaton splitter;

	JavaTokenizer() {
		splitter = new RunAutomaton(new RegExp(splitterRegex).toAutomaton());
	}

	@Override
	public Iterable<Tokenizer.SnippetWithBaseline> tokenize(String file) {
		int currentLineNumber = 0;
		int lastStart = 0;

		Collection<Tokenizer.SnippetWithBaseline> ret = Lists.newArrayList();
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
				ret.add(new Tokenizer.SnippetWithBaseline(currentLineNumber, currentFunctionString));
			}

			lastStart = start;
			currentLineNumber += countOccurrences(currentFunctionString, '\n');
		}
		String currentFunctionString = file.substring(lastStart, file.length());
		ret.add(new SnippetWithBaseline(currentLineNumber, currentFunctionString));
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
