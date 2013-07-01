package ch.unibe.scg.cc.mappers;

import org.mozilla.universalchardet.UniversalDetector;

class CharsetDetector {
	final UniversalDetector detector = new UniversalDetector(null);

	public String charsetOf(byte[] bytes) {
		detector.handleData(bytes, 0, bytes.length);
		detector.dataEnd();
		String encoding = detector.getDetectedCharset();
		detector.reset();
		return encoding == null ? "ASCII" : encoding;
	}
}