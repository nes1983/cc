package ch.unibe.scg.cc.activerecord;

public interface RealProjectFactory {
	RealProject create(String projectName, Version version, int versionNumber);
}
