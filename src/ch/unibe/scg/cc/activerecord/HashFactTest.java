package ch.unibe.scg.cc.activerecord;

import org.junit.runner.RunWith;

import ch.unibe.jexample.JExample;
import ch.unibe.scg.cc.Main;
import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;
import com.google.inject.Injector;

@RunWith(JExample.class)
public class HashFactTest {
	
	public static void main(String... args) {
		Injector injector = Guice.createInjector(new JavaModule(), new CCModule());
		HashFact hashFact = injector.getInstance(HashFact.class);
		
		hashFact.setHash(new byte[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20});
		Function function = new Function();
		function.setBaseLine(1);
		function.setContents("abcde");
		function.setFile_path("/root/test/poppins.java");
		hashFact.setFunction(function);
		Project project = new Project();
		project.setName("Mary Poppins");
		hashFact.setProject(project);
		Location location = new Location();
		location.setFirstLine(2);
		hashFact.setLocation(location);
		hashFact.setType(Main.TYPE_1_CLONE);
		hashFact.save();
	}
	
	
}
