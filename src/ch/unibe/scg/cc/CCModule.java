package ch.unibe.scg.cc;

import java.security.MessageDigest;

import com.google.inject.AbstractModule;

//@formatter:off
public class CCModule extends AbstractModule {
	@Override
	protected void configure() {
		bind(MessageDigest.class).toProvider(MessageDigestProvider.class);
	}
}
