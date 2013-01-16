package ch.unibe.scg.cc.modules;

import java.security.MessageDigest;
import java.util.Comparator;
import java.util.Set;

import javax.inject.Singleton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.MessageDigestProvider;
import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.ConfigurationProvider;
import ch.unibe.scg.cc.activerecord.Function.FunctionFactory;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.RealCodeFile;
import ch.unibe.scg.cc.activerecord.RealCodeFileFactory;
import ch.unibe.scg.cc.activerecord.RealProject;
import ch.unibe.scg.cc.activerecord.RealProjectFactory;
import ch.unibe.scg.cc.activerecord.RealVersion;
import ch.unibe.scg.cc.activerecord.RealVersionFactory;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.util.ByteSetProvider;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;

public class CCModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(MessageDigest.class).toProvider(MessageDigestProvider.class).in(Singleton.class);
		bind(Configuration.class).toProvider(ConfigurationProvider.class).in(Singleton.class);
		bind(new TypeLiteral<Comparator<byte[]>>() {
		}).toInstance(Bytes.BYTES_COMPARATOR);
		bind(new TypeLiteral<Set<byte[]>>() {
		}).toProvider(ByteSetProvider.class);

		// factories
		install(new FactoryModuleBuilder().implement(Project.class, RealProject.class).build(RealProjectFactory.class));
		install(new FactoryModuleBuilder().implement(Version.class, RealVersion.class).build(RealVersionFactory.class));
		install(new FactoryModuleBuilder().implement(CodeFile.class, RealCodeFile.class).build(
				RealCodeFileFactory.class));
		install(new FactoryModuleBuilder().build(FunctionFactory.class));
	}
}
