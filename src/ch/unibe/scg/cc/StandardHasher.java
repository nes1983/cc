package ch.unibe.scg.cc;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;

import javax.inject.Inject;

public class StandardHasher implements Hasher {
	@Inject
	MessageDigest md;

	public byte[] hash(String document) {
		byte[] ret;
		try {
			ret = md.digest(document.getBytes("UTF-16LE"));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
		md.reset();
		return ret;
	}
}
