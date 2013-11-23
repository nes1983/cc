package ch.unibe.scg.cc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;

enum Utils {
	; // Don't instantiate

	static int countLines(String str) {
		LineNumberReader lnr = new LineNumberReader(new StringReader(checkNotNull(str)));
		try {
			lnr.skip(Long.MAX_VALUE);
		} catch (IOException e) {
			throw new RuntimeException(e); // Can't happen.
		}

		// getLineNumber() returns the number of line terminators, therefore we
		// have to add one to get the correct number of lines.
		return lnr.getLineNumber() + 1;
	}

	/**
	 * Performs hash = hash XOR otherHash. Changes hash in place.
	 */
	static void xor(byte[] hash, byte[] otherHash) {
		for (int i = 0; i < hash.length; i++) {
			hash[i] ^= otherHash[i]; // Beware of the implicit narrowing cast here. But it works out.
		}
	}
}
