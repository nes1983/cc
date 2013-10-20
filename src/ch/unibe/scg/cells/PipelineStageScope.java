package ch.unibe.scg.cells;

import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Scope;

/** The scope behind {@link PipelineScoped}. */
// The implementation follows Guice's Scopes.SINGLETON class.
class PipelineStageScope implements Scope {
	// Weak because the life-cycle of providers is up to Guice, and not to us,
	// and we have no use for them once Guice has given them up.
	final private WeakHashMap<ScopedProvider<?>, Void> scopedProviders = new WeakHashMap<>();

	/* Exits a scope. This method is reentrant, but should not be called concurrently. */
	void exit() {
		for (ScopedProvider<?> e : scopedProviders.keySet()) {
			synchronized(e) {
				e.values.clear();
			}
		}
	}

	/** The produced providers are threadsafe. */
	@Override
	public <T> Provider<T> scope(Key<T> key, Provider<T> unscoped) {
		ScopedProvider<T> ret = new ScopedProvider<>(key, unscoped);
		scopedProviders.put(ret, null);
		return ret;
	}

	// TODO: should this be serializable?
	private static class ScopedProvider<T> implements Provider<T> {
		final Map<Key<?>, Object> values = new HashMap<>();

		final Key<T> key;
		final Provider<T> unscoped;

		ScopedProvider(Key<T> key, Provider<T> unscoped) {
			this.key = key;
			this.unscoped = unscoped;
		}

		@Override public synchronized T get() {
			if (!values.containsKey(key)) {
				values.put(key, unscoped.get());
			}
			return (T) values.get(key);
		}
	}
}
