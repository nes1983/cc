package ch.unibe.scg.cc;

import java.security.MessageDigest;
import java.util.Comparator;

import ch.unibe.scg.cc.Protos.Clone;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;

//@formatter:off
public class CCModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(MessageDigest.class).toProvider(MessageDigestProvider.class);

		bind(new TypeLiteral<Comparator<Clone>>() {}).to(CloneComparator.class);
	}
}
