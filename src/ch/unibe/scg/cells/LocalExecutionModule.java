package ch.unibe.scg.cells;

import com.google.inject.AbstractModule;

/** A module that configures a {@link Pipeline} for local (non-distributed) execution. */
public final class LocalExecutionModule extends AbstractModule {
	@Override
	protected void configure() {
		 PipelineStageScope pipeScope = new PipelineStageScope();
		 bindScope(PipelineStageScoped.class, pipeScope);
		 bind(PipelineStageScope.class).toInstance(pipeScope);
	}
}
