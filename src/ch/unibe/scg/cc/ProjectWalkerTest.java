package ch.unibe.scg.cc;

import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.VFS;
import org.junit.Test;
import org.junit.runner.RunWith;

import ch.unibe.jexample.JExample;
import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Module;

import ch.unibe.jexample.*;

@RunWith(JExample.class)
public class ProjectWalkerTest {
	final Connection connection = mock(Connection.class);
	final PreparedStatement preparedStatement = mock(PreparedStatement.class);
	final ResultSet resultSet = mock(ResultSet.class);
	
	@Test
	public ProjectWalker makeProjectWalker() {

		ProjectWalker walker = Guice.createInjector(testingModule(),
				new CCModule(), new JavaModule()).getInstance(ProjectWalker.class);
		assertThat(walker.projectProvider, notNullValue());
		return walker;
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
