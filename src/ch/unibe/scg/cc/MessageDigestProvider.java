package ch.unibe.scg.cc;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.google.inject.Provider;

class MessageDigestProvider implements Provider<MessageDigest> {
	@Override
	public MessageDigest get() {
		try {
			return MessageDigest.getInstance("SHA1");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}
}
