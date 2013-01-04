package ch.unibe.scg.cc.util;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import javax.inject.Inject;
import javax.inject.Provider;

public class ByteSetProvider implements Provider<Set<byte[]>> {

	final Provider<Comparator<byte[]>> comparatorProvider;

	@Inject
	public ByteSetProvider(Provider<Comparator<byte[]>> comparatorProvider) {
		super();
		this.comparatorProvider = comparatorProvider;
	}

	@Override
	public Set<byte[]> get() {
		return new TreeSet<byte[]>(comparatorProvider.get());
	}

}
