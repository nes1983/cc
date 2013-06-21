package ch.unibe.scg.cc.activerecord;

import com.google.inject.assistedinject.Assisted;

public interface ProjectFactory {
	Project create(@Assisted("name") String projectName, @Assisted Version version, @Assisted("tag") String tag);
}
