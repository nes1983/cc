package ch.unibe.scg.cc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLClientInfoException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import ch.unibe.jexample.Given;
import ch.unibe.jexample.JExample;
import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Module;

import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.*;

@RunWith(JExample.class)
public class FrontendTest {

	final Connection connection = mock(Connection.class);
	
	@Test
	public Frontend makeFrontend() {

		Frontend frontend = Guice.createInjector(testingModule(), new CCModule(), new JavaModule()).getInstance(Frontend.class);
		assertThat(frontend.tokenizer, notNullValue());
		return frontend;
	}
	
	@Given("makeFrontend")
	public Frontend testNormalize1(Frontend frontend) {
		String normalized = frontend.type1NormalForm("\npublic    static void doIt(char[] arg) {\n");
		assertThat(normalized, is("\nstatic void doIt(char[] arg) {\n"));
		return frontend;
	}
	
	@Given("testNormalize1") 
	public Frontend testNormalize2(Frontend frontend) {
		String normalized = frontend.type2NormalForm("\npublic    static void doIt(char[] arg) {\n");
		assertThat(normalized, is("\nt t t(t[] t) {\n"));
		return frontend;
	}
	
	Module testingModule() {
		return new AbstractModule() {

			@Override
			protected void configure() {
				bind(Connection.class).toInstance(connection);	
			}
		};
	}

}
