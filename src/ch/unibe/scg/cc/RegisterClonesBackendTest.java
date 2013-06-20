package ch.unibe.scg.cc;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.mapreduce.Counter;
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

import com.google.inject.Guice;
import com.google.inject.Injector;

@RunWith(JExample.class)
public class RegisterClonesBackendTest {

	Project project;
	final HTable htable = mock(HTable.class);
	Function function;
	final StringOfLinesFactory stringOfLinesFactory = new StringOfLinesFactory();
	final StringOfLines sampleLines = stringOfLinesFactory.make("a\nb\nc\nd\ne\nf\n");
	final StringOfLines aThruF = stringOfLinesFactory.make("a\nb\nc\nd\ne\nf\n");
	final StringOfLines aThruK = stringOfLinesFactory.make("a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk\n");

	@Test
	public RegisterClonesBackend testOneRegister() {
		CloneRegistry cr = mock(CloneRegistry.class);
		Injector i = Guice.createInjector(new CCModule());
		StandardHasher sth = i.getInstance(StandardHasher.class);
		ShingleHasher shh = i.getInstance(ShingleHasher.class);

		RegisterClonesBackend rcb = new RegisterClonesBackend(cr, sth, shh, stringOfLinesFactory);
		rcb.successfullyHashedCounter = mock(Counter.class);
		rcb.cannotBeHashedCounter = mock(Counter.class);

		project = mock(Project.class);
		function = mock(Function.class);

		rcb.registerConsecutiveLinesOfCode(sampleLines, function, Main.TYPE_3_CLONE);

		verify(cr).register(
				eq(new byte[] { 69, 10, -93, -20, 53, -4, -66, -128, 103, -28, 44, 42, 38, -9, 20, -75, -38, 89, -71,
						70 }), (String) anyObject(), eq(function), eq(0), eq(5), eq(Main.TYPE_3_CLONE));

		Mockito.reset(cr); // JExample doesn't support @After
		return rcb;
	}

	@Given("testOneRegister")
	public RegisterClonesBackend testMoreRegisters(RegisterClonesBackend rcb) {
		CloneRegistry cr = mock(CloneRegistry.class);
		rcb.registry = cr;
		rcb.registerConsecutiveLinesOfCode(aThruF, function, Main.TYPE_3_CLONE);
		verify(cr).register(
				eq(new byte[] { 69, 10, -93, -20, 53, -4, -66, -128, 103, -28, 44, 42, 38, -9, 20, -75, -38, 89, -71,
						70 }), (String) anyObject(), eq(function), eq(0), eq(5), eq(Main.TYPE_3_CLONE));
		Mockito.reset(cr); // JExample doesn't support @After
		return rcb;
	}

	@Given("testMoreRegisters")
	public RegisterClonesBackend testLotsOfRegisters(RegisterClonesBackend rcb) {
		CloneRegistry cr = mock(CloneRegistry.class);
		rcb.registry = cr;
		rcb.registerConsecutiveLinesOfCode(aThruK, function, Main.TYPE_2_CLONE);
		verify(cr, times(6)).register(((byte[]) anyObject()), (String) anyObject(), eq(function), anyInt(), anyInt(),
				eq(Main.TYPE_2_CLONE));
		Mockito.reset(cr); // JExample doesn't support @After

		return rcb;
	}

}