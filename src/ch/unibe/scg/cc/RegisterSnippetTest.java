package ch.unibe.scg.cc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.SQLException;

import javax.inject.Provider;

import org.junit.Test;

import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Snippet;

public class RegisterSnippetTest {

	@Test
	public void addSnippet() throws SQLException, IOException {
		final Provider<Snippet> snippetProvider = (Provider<Snippet>) mock(Provider.class);
		final Snippet snippet = mock(Snippet.class);
		when(snippetProvider.get()).thenReturn(snippet);

		CloneRegistry registry = new CloneRegistry(snippetProvider, null, null);
		Function function = new Function(null, 0, "", "");

		byte[] bytes = new byte[] { 0 };
		registry.register(bytes, "", function, null, (byte) 0x03);

		verify(snippet).setType((byte) 0x03);
	}

}
