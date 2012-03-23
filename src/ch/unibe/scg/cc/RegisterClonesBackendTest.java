package ch.unibe.scg.cc;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.sql.Connection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import ch.unibe.jexample.Given;
import ch.unibe.jexample.JExample;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.lines.StringOfLines;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;
import ch.unibe.scg.cc.modules.CCModule;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Module;
import com.google.inject.util.Modules;

@RunWith(JExample.class)
public class RegisterClonesBackendTest {
	
	Project project;
	final CloneRegistry cloneRegistry = mock(CloneRegistry.class);
	final Connection connection = mock(Connection.class);
	Function function;
	final StringOfLinesFactory stringOfLinesFactory = new StringOfLinesFactory();
	final StringOfLines sampleLines = stringOfLinesFactory.make("a\nb\nc\nd\ne\nf\n"); 
	final StringOfLines aThruF = stringOfLinesFactory.make("a\nb\nc\nd\ne\nf\n");
	final StringOfLines aThruK = stringOfLinesFactory.make("a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\n"); 


	
	@Test
	public RegisterClonesBackend testOneRegister() {

		Module t = new AbstractModule() {
			protected void configure() {
				bind(CloneRegistry.class).toInstance(cloneRegistry);
				bind(Connection.class).toInstance(connection);
			}
		};
		Module tt = Modules.override(new CCModule()).with(t);
		RegisterClonesBackend rcb = Guice.createInjector(tt).getInstance(RegisterClonesBackend.class);
		
		project = mock(Project.class);
		function = mock(Function.class);
		stub(function.getBaseLine()).toReturn(3);
		
		rcb.registerConsecutiveLinesOfCode(sampleLines, function, Main.TYPE_3_CLONE);
		
		verify(cloneRegistry).register(eq(new byte[] {69, 10, -93, -20, 53, -4, -66, -128, 103, -28, 44, 42, 38, -9, 20, -75, -38, 89, -71, 70}),  eq(function),
				eq(3+0), eq(5), eq(Main.TYPE_3_CLONE));

		Mockito.reset(cloneRegistry); //JExample doesn't support @After
		return rcb;
	}
	
	
	
	@Given("testOneRegister")
	public RegisterClonesBackend testMoreRegisters(RegisterClonesBackend rcb) {
		rcb.registerConsecutiveLinesOfCode(aThruF, function, Main.TYPE_3_CLONE);
		verify(cloneRegistry).register(eq(new byte[] {69, 10, -93, -20, 53, -4, -66, -128, 103, -28, 44, 42, 38, -9, 20, -75, -38, 89, -71, 70}),  eq(function),
				eq(3), eq(5), eq(Main.TYPE_3_CLONE));
		verify(cloneRegistry).register(eq(new byte[] {11, 94, -59, -112, 63, 8, -52, -68, 86, -65, 105, 103, -53, -106, -96, -11, 25, 50, -98, -25}), eq(function),
				eq(4), eq(5), eq(Main.TYPE_3_CLONE));
		Mockito.reset(cloneRegistry); //JExample doesn't support @After
		return rcb;
	}
	
	@Given("testMoreRegisters")
	public RegisterClonesBackend testLotsOfRegisters(RegisterClonesBackend rcb) {
		rcb.registerConsecutiveLinesOfCode(aThruK, function, Main.TYPE_2_CLONE);
		verify(cloneRegistry, times(7)).register(((byte[]) anyObject()), eq(function), anyInt(), anyInt(), eq(Main.TYPE_2_CLONE));
		Mockito.reset(cloneRegistry); //JExample doesn't support @After

		return rcb;
	}

}