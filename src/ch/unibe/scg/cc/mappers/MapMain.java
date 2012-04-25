package ch.unibe.scg.cc.mappers;

import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class MapMain {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws Exception {
		assert args.length == 1;
		Class c = Class.forName(args[0]);
		Injector injector = Guice.createInjector(new CCModule(), new JavaModule());
		Object instance = injector.getInstance(c);
		((Runnable) instance).run();
	}
}
