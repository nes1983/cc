package ch.unibe.scg.cc.lines;

import java.util.ArrayList;
import java.util.List;

public class ModifiableLinesFactory {
	ModifiableLines make(StringBuilder stringBuilder, char separator) {
		String string = stringBuilder.toString();
		if (!string.endsWith(String.valueOf(separator))) {
			stringBuilder.append('\n');
		}
		return makeSanitized(stringBuilder, separator);
	}

	ModifiableLines makeSanitized(StringBuilder stringBuilder, char separator) {
		String string = stringBuilder.toString();
		List<LineBreak> lineBreaks = new ArrayList<LineBreak>();
		// int count = JavaTokenizer.countOccurrences(string, separator);
		// int[] separators = new int[count + 1];
		lineBreaks.add(new LineBreak(0, 1));
		int thisIndex = string.indexOf(separator);

		for (; thisIndex != -1; thisIndex = string.indexOf(separator,
				thisIndex + 1)) {
			lineBreaks.add(new LineBreak(thisIndex, 1));
		}
		return new ModifiableLines(stringBuilder, lineBreaks);

	}

	ModifiableLines make(StringBuilder string) {
		return this.make(string, '\n');
	}

}