package ch.unibe.scg.cells;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.inject.Provider;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

/** A module that configures a {@link Pipeline} for local (non-distributed) execution. */
public final class LocalExecutionModule extends AbstractModule {
	private static class CounterRegistryProvider implements Provider<Set<LocalCounter>> {
		@Override
		public Set<LocalCounter> get() {
			// Linked hash set to make iteration predictable in unit tests.
			return Collections.synchronizedSet(new LinkedHashSet<LocalCounter>());
		}
	}

	@Override
	protected void configure() {
		 PipelineStageScope pipeScope = new PipelineStageScope();
		 bindScope(PipelineStageScoped.class, pipeScope);
		 bind(PipelineStageScope.class).toInstance(pipeScope);

		 bind(new TypeLiteral<Set<LocalCounter>>() {})
		 	.toProvider(CounterRegistryProvider.class)
		 	.in(PipelineStageScoped.class);
	}
}
