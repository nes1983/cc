package ch.unibe.scg.cc;

public class ByteUtils {
	/** 20 bytes each with hex-value \x00 */
	public static final byte[] EMPTY_SHA1_KEY = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

	/**
	 * Compute the bitwise XOR of two arrays of bytes. The arrays have to be of
	 * same length.
	 *
	 * @param x1
	 *            the first array
	 * @param x2
	 *            the second array
	 * @return x1 XOR x2
	 */
	public static byte[] xor(byte[] x1, byte[] x2) {
		assert x1.length == x2.length;
		byte[] out = new byte[x1.length];

		for (int i = x1.length - 1; i >= 0; i--) {
			out[i] = (byte) (x1[i] ^ x2[i]);
		}
		return out;
	}

	/** @return the byte array in a human-readable string */
	public static String bytesToHex(byte[] bytes) {
		final char[] hexArray = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
		char[] hexChars = new char[bytes.length * 2];
		int v;
		for (int j = 0; j < bytes.length; j++) {
			v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}
}
