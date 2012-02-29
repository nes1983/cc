package ch.unibe.scg.cc;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.junit.After;
import org.junit.Before;
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

import static org.mockito.Mockito.*;

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
		
		rcb.registerConsecutiveLinesOfCode(sampleLines, project, function, Main.TYPE_3_CLONE);
		
		verify(cloneRegistry).register(eq(new byte[] {69, 10, -93, -20, 53, -4, -66, -128, 103, -28, 44, 42, 38, -9, 20, -75, -38, 89, -71, 70}),  eq(project),  eq(function),
				eq(3+0), eq(5), eq(Main.TYPE_3_CLONE));

		Mockito.reset(cloneRegistry); //JExample doesn't support @After
		return rcb;
	}
	
	
	
	@Given("testOneRegister")
	public RegisterClonesBackend testMoreRegisters(RegisterClonesBackend rcb) {
		rcb.registerConsecutiveLinesOfCode(aThruF, project, function, Main.TYPE_3_CLONE);
		verify(cloneRegistry).register(eq(new byte[] {69, 10, -93, -20, 53, -4, -66, -128, 103, -28, 44, 42, 38, -9, 20, -75, -38, 89, -71, 70}),  eq(project),  eq(function),
				eq(3), eq(5), eq(Main.TYPE_3_CLONE));
		verify(cloneRegistry).register(eq(new byte[] {-19, -102, -45, -43, 62, 36, 123, 88, -108, -2, 109, 11, 49, 14, -93, -90, 113, -45, -44, 48}),  eq(project),  eq(function),
				eq(3), eq(6), eq(Main.TYPE_3_CLONE));
		verify(cloneRegistry).register((byte[])anyObject(),  eq(project),  eq(function),
				eq(4), eq(5), eq(Main.TYPE_3_CLONE));
		Mockito.reset(cloneRegistry); //JExample doesn't support @After
		return rcb;
	}
	
	@Given("tesMoreRegisters")
	public RegisterClonesBackend testLotsOfRegisters(RegisterClonesBackend rcb) {
		rcb.registerConsecutiveLinesOfCode(aThruK, project, function, Main.TYPE_2_CLONE);
		verify(cloneRegistry, times(25)).register(((byte[]) anyObject()), eq(project), eq(function), anyInt(), anyInt(), eq(Main.TYPE_2_CLONE));
		Mockito.reset(cloneRegistry); //JExample doesn't support @After

		return rcb;
	}

}
