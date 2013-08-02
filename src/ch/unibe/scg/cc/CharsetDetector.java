package ch.unibe.scg.cc;

import java.nio.charset.Charset;

import org.mozilla.universalchardet.UniversalDetector;

import com.google.common.base.Charsets;

/** Detects the charset of strings */
class CharsetDetector {
	final UniversalDetector detector = new UniversalDetector(null);

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
}