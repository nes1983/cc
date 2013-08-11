package ch.unibe.scg.cc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.security.MessageDigest;

import javax.inject.Inject;

import com.google.common.base.Charsets;

/** Performs no normalization. Simply computes the cryptographic hash of a string */
public class StandardHasher implements Hasher {
	final private static long serialVersionUID = 1L;

	transient MessageDigest md;

	@Inject
	StandardHasher(MessageDigest md) {
		this.md = md;
	}

	@Override
	public byte[] hash(String document) {
		checkNotNull(document);

		return md.digest(document.getBytes(Charsets.UTF_8));
	}

	private void readObject(java.io.ObjectInputStream stream) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();
		md = new MessageDigestProvider().get();
	}
}
