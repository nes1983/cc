package ch.unibe.scg.cc.activerecord;

import com.google.inject.assistedinject.Assisted;

public interface RealProjectFactory {
	RealProject create(@Assisted("name") String projectName,
			@Assisted Version version, @Assisted("tag") String tag);
}
