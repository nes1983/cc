package ch.unibe.scg.cc;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Location;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.Version;

public interface Registry {
	public void register(Project project);

	public void register(Version version);

	public void register(CodeFile codeFile);

	public void register(Function function);

	public void register(byte[] hash, String snippet, Function function, int from, int length, byte type);

	public void register(byte[] hash, String snippetValue, Function function, Location location, byte type);
}
