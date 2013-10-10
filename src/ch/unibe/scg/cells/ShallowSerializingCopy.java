package ch.unibe.scg.cells;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

/**
 * An ObjectOutputStream that allows making shallow copies of objects.
 *
 * <p>
 * Ordinarily, using serialization to copy objects naturally leads to deep copies.
 * To circumvent this, and allow shallow copies via serializations, we have to `smuggle` the
 * live objects around the serialization format.
 *
 * <p>
 * Now, obviously, this breaks the entire purpose of serialization: to have everything encoded in
 * binary. However, it can come in handy if serialization is merely used to make copies of objects,
 * as is the case in the InMemoryPipeline, which uses serialization to copy mappers.
 *
 * <p>
 * Please <em>never</em> set this class to public. Don't talk about it, either.
 */
class ShallowSerializingCopy {
	/**
	 * Use {@link ShallowSerializingCopy} to smuggle ourselves around serialization.
	 * InMemoryShufflers should NOT get copied while the mappers that hold them get copied.
	 */
	static class SerializableLiveObject implements Serializable {
		final private static long serialVersionUID = 1L;
		final private static String error = "This class can only be serialized in a "
				+ ShallowSerializingCopy.class.getName()
				+ "If you're trying to serialize a table, consider using one from "
				+ "the ch.unibe.chscg.cells.hadoop package.";

		private transient Object liveObject;

		SerializableLiveObject(Object liveObject) {
			this.liveObject = liveObject;
		}

		private void writeObject(ObjectOutputStream out) throws IOException {
			if (!(out instanceof ShallowSerializingCopy.LiveObjectOutputStream)) {
				throw new UnsupportedOperationException(error + liveObject.toString());
			}

			LiveObjectOutputStream shallowOut = (ShallowSerializingCopy.LiveObjectOutputStream) out;
			shallowOut.writeLiveObject(liveObject);
		}

		private void readObject(java.io.ObjectInputStream in) throws IOException {
			if (!(in instanceof LiveObjectInputStream)) {
				throw new UnsupportedOperationException(error + liveObject.toString());
			}
			LiveObjectInputStream shallowIn = (LiveObjectInputStream) in;
			liveObject = shallowIn.readLiveObject();
		}

		private Object readResolve() {
			assert liveObject != null;
			return liveObject;
		}
	}

	private static class LiveObjectOutputStream extends ObjectOutputStream {
		final private SecureRandom random = new SecureRandom();
		final private Map<Long, Object> liveObjects;


		LiveObjectOutputStream(OutputStream out, Map<Long, Object> liveObjects) throws IOException {
			super(out);
			this.liveObjects = liveObjects;
		}

		void writeLiveObject(Object o) throws IOException {
			long key = random.nextLong();
			writeLong(key);
			liveObjects.put(key, o);
		}
	}

	private static class LiveObjectInputStream extends ObjectInputStream {
		final private Map<Long, Object> liveObjects;

		LiveObjectInputStream(InputStream in, Map<Long, Object> liveObjects) throws IOException {
			super(in);
			this.liveObjects = liveObjects;
		}

		Object readLiveObject() throws IOException {
			long key = readLong();
			return liveObjects.get(key);
		}
	}

	/** Use serialization to clone a mapper. */
	static <T> T clone(T in) throws IOException {
		ByteArrayOutputStream bOut = new ByteArrayOutputStream();
		Map<Long, Object> smugglerCache = new HashMap<>();
		try (ObjectOutputStream out = new LiveObjectOutputStream(bOut, smugglerCache)) {
			out.writeObject(in);
		}
		try {
			return (T)  new LiveObjectInputStream(
					new ByteArrayInputStream(bOut.toByteArray()),
					smugglerCache)
				.readObject();
		} catch (ClassNotFoundException e) {
			throw new AssertionError("We just serialized this object a minute ago. The class cannot be missing", e);
		}
	}
}
