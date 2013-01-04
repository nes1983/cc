package ch.unibe.scg.cc.git;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.jgit.lib.ObjectId;

public class PackedRefParser {

	final static Pattern pattern = Pattern
			.compile("([a-f0-9]{40}) refs/tags/(.+)");

	public List<PackedRef> parse(InputStream ins) throws IOException {
		int ch;
		StringBuilder content = new StringBuilder();
		while ((ch = ins.read()) != -1)
			content.append((char) ch);

		return parse(content.toString());
	}

	private List<PackedRef> parse(String content) {
		List<PackedRef> list = new ArrayList<PackedRef>();
		Scanner s = new Scanner(content);
		while (s.hasNextLine()) {
			String line = s.nextLine();
			Matcher m = pattern.matcher(line);
			if (m.matches()) {
				String sha = m.group(1);
				assert sha.length() == 40;
				ObjectId key = ObjectId.fromString(sha);
				String name = m.group(2);
				PackedRef pr = new PackedRef(key, name);
				list.add(pr);
			}
		}
		return list;
	}

}
