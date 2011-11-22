package ch.unibe.scg.cc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;

import javax.inject.Provider;

import org.junit.Test;

import ch.unibe.scg.cc.activerecord.HashFact;

public class RegisterSnippetTest {

	@Test
	public void addSnippet() throws SQLException {
		final Provider<HashFact> hashFactProvider = (Provider<HashFact>) mock(Provider.class);
		final HashFact hashFact = mock(HashFact.class);
		when(hashFactProvider.get()).thenReturn(hashFact);

		CloneRegistry registry = new CloneRegistry(hashFactProvider,null);


		byte[] bytes = new byte[] { 0 };
		registry.register(bytes, null, null, null, 3);

		verify(hashFact).save();

		verify(hashFact).setType(3);
	}

}
