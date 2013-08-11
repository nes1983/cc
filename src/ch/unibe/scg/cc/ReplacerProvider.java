package ch.unibe.scg.cc;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import ch.unibe.scg.cc.regex.Replace;

import com.google.inject.Provider;

/** Extend this class and implement parameterless arguments starting with "make" to add more replaces */
public abstract class ReplacerProvider implements Provider<Replace[]>, Serializable {
	final private static long serialVersionUID = 1L;

	final private Replace[] type = new Replace[] {};

	@Override
	public final Replace[] get() {
		List<Replace> ret = new ArrayList<>();
		Method[] methods = this.getClass().getMethods();

		// Making the output deterministic helps testing.
		Arrays.sort(methods, new Comparator<Method>() {
			@Override public int compare(Method m1, Method m2) {
				return m1.getName().compareTo(m2.getName());
			}
		});

		assert methods.length > 0;
		for (Method method : methods) {
			if (!method.getName().startsWith("make")) {
				continue;
			}
			Replace r = makeReplace(method);
			ret.add(r);
		}
		return ret.toArray(type);
	}

	final Replace makeReplace(Method method) {
		try {
			return (Replace) method.invoke(this);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("Methods named 'make' should not accept arguments.", e);
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}
	}
}
