package ch.unibe.scg.cc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.SQLException;

import javax.inject.Provider;

import org.junit.Test;

import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.HashFact;

public class RegisterSnippetTest {

	@Test
	public void addSnippet() throws SQLException, IOException {
		final Provider<HashFact> hashFactProvider = (Provider<HashFact>) mock(Provider.class);
		final HashFact hashFact = mock(HashFact.class);
		when(hashFactProvider.get()).thenReturn(hashFact);

		CloneRegistry registry = new CloneRegistry(hashFactProvider,null);
		Function function = new Function();

		byte[] bytes = new byte[] { 0 };
		registry.register(bytes, "", function, null, 3);

		verify(hashFact).setType(3);
	}

}
