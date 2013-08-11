package ch.unibe.scg.cc;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;

import org.mozilla.universalchardet.UniversalDetector;

import com.google.common.base.Charsets;

/** Detects the charset of strings */
class CharsetDetector implements Serializable {
	private static final long serialVersionUID = 1L;
	private transient UniversalDetector detector = new UniversalDetector(null);

	/** @return the charset if it could be guessed; "US_ASCII" otherwise. */
	Charset charsetOf(byte[] bytes) {
		detector.handleData(bytes, 0, bytes.length);
		detector.dataEnd();
		String encoding = detector.getDetectedCharset();
		detector.reset();
		if (encoding == null) {
			return Charsets.US_ASCII;
		}
		return Charset.forName(encoding);
	}

	private void readObject(java.io.ObjectInputStream stream) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();
		detector = new UniversalDetector(null);
	}
}