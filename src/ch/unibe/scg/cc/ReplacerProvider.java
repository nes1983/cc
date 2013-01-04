package ch.unibe.scg.cc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Singleton;

import org.apache.commons.lang3.ArrayUtils;

import ch.unibe.scg.cc.regex.Replace;

import com.google.inject.Provider;

@Singleton
public abstract class ReplacerProvider implements Provider<Replace[]> {

	final Replace[] type = new Replace[] {};

	@Override
	public Replace[] get() {
		List<Replace> ret = new ArrayList<Replace>();
		Method[] methods = this.getClass().getMethods();
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
			throw new RuntimeException(
					"Methods named 'make' should not accept arguments.", e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e);
		}
		assert r != null;
		return r;
	}

}
