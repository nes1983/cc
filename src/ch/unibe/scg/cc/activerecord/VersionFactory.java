package ch.unibe.scg.cc.activerecord;

public interface VersionFactory {
	Version create(String filePath, CodeFile codeFile);
}
