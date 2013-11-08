package ch.unibe.scg.cells.hadoop;

import org.junit.runner.JUnitCore;

/**
 * Hadoop has its own JUnit in its classpath, whose class loader cannot find our classes.
 * This little wrapper forces to load our JUnit, which has the right class loader attached to it
 * and will work.
 */
public final class JUnitRunner {
	/** Forward all arguments to {@link JUnitCore}. */
	public static void main(String[] args) throws ClassNotFoundException {
		Class<?>[] junitArgs = new Class<?>[args.length];
		for (int i = 0; i < args.length; i++) {
			junitArgs[i] = Class.forName(args[i]);
		}
		JUnitCore.runClasses(junitArgs);
	}
}
