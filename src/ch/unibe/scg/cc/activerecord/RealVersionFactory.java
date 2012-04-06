package ch.unibe.scg.cc.activerecord;

public interface RealVersionFactory {
	RealVersion create(String filePath, CodeFile codeFile);
}
