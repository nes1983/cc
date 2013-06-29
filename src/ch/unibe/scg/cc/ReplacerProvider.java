package ch.unibe.scg.cc;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import javax.inject.Singleton;

import ch.unibe.scg.cc.regex.Replace;

import com.google.common.collect.Lists;
import com.google.inject.Provider;

@Singleton
public abstract class ReplacerProvider implements Provider<Replace[]> {

	final Replace[] type = new Replace[] {};
	final MethodComparator methodComparator = new MethodComparator();

	@Override
	public Replace[] get() {
		List<Replace> ret = Lists.newArrayList();
		Method[] methods = this.getClass().getMethods();
		Arrays.sort(methods, methodComparator);
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

	Replace makeReplace(Method method) {
		Replace r;
		try {
			r = (Replace) method.invoke(this);
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("Methods named 'make' should not accept arguments.", e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e);
		}
		assert r != null;
		return r;
	}

	/** lexicographic comparison of method names */
	static class MethodComparator implements Comparator<Method>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public int compare(Method m1, Method m2) {
			return m1.getName().compareTo(m2.getName());
		}
	}
}
