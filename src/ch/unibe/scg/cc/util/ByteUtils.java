package ch.unibe.scg.cc.util;

public class ByteUtils {
    /**
     * Compute the bitwise XOR of two arrays of bytes. The arrays have to be of
     * same length.
     * 
     * @param x1 the first array
     * @param x2 the second array
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
}
