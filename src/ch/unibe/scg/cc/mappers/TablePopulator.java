package ch.unibe.scg.cc.mappers;


import java.io.IOException;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.mozilla.universalchardet.UniversalDetector;

import ch.unibe.scg.cc.Frontend;
import ch.unibe.scg.cc.Java;
import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.RealProjectFactory;
import ch.unibe.scg.cc.activerecord.RealVersionFactory;
import ch.unibe.scg.cc.activerecord.Version;

/** Use GitTablePopulator instead of TablePopulator! */
public class TablePopulator implements Runnable {
	static Logger logger = Logger.getLogger(TablePopulator.class.getName());
	final MRWrapper mrWrapper;

	@Inject
	TablePopulator(MRWrapper mrWrapper) {
		this.mrWrapper = mrWrapper;
	}

	public void run() {
		throw new UnsupportedOperationException("Use GitTablePopulator - TablePopulator is no longer supported.");
	}

	public static class TablePopulatorMapper extends GuiceMapper<Text, BytesWritable, Text, IntWritable> {
		@Inject
		TablePopulatorMapper(@Java Frontend javaFrontend, @Named("versions") HTable versions,
				@Named("files") HTable files, @Named("functions") HTable functions, @Named("facts") HTable facts,
				@Named("strings") HTable strings, @Named("hashfactContent") HTable hashfactContent,
				RealProjectFactory projectFactory, RealVersionFactory versionFactory, CharsetDetector charsetDetector) {
			super();
			this.javaFrontend = javaFrontend;
			this.versions = versions;
			this.files = files;
			this.functions = functions;
			this.facts = facts;
			this.strings = strings;
			this.hashfactContent = hashfactContent;
			this.projectFactory = projectFactory;
			this.versionFactory = versionFactory;
			this.charsetDetector = charsetDetector;
		}

		final Frontend javaFrontend;
		final HTable versions, files, functions, facts, strings, hashfactContent;
		final RealProjectFactory projectFactory;
		final RealVersionFactory versionFactory;
		final CharsetDetector charsetDetector;

		@Override
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			if (value.getLength() == 0) {
				return;
			}

			byte[] bytes = value.getBytes();
			String content = new String(bytes, charsetDetector.charsetOf(bytes));

			String filePath = key.toString();
			String projectName = filePath.substring(0, filePath.indexOf('/'));
			String fileName = filePath.substring(filePath.lastIndexOf('/') + 1);

			CodeFile codeFile = register(content, fileName);
			Version version = register(filePath, codeFile);
			register(projectName, version, 1);
		}

		private CodeFile register(String content, String fileName) throws IOException {
			CodeFile codeFile = javaFrontend.register(content, fileName);
			functions.flushCommits();
			facts.flushCommits();
			hashfactContent.flushCommits();
			return codeFile;
		}

		private Version register(String filePath, CodeFile codeFile) throws IOException {
			Version version = versionFactory.create(filePath, codeFile);
			javaFrontend.register(version);
			files.flushCommits();
			return version;
		}

		private void register(String projectName, Version version, int versionNumber) throws IOException {
			Project proj = projectFactory.create(projectName, version, "1"); // TODO
																				// get
																				// versionNumber
			javaFrontend.register(proj);
			versions.flushCommits();
			strings.flushCommits();
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			javaFrontend.close();
		}
	}

	public static class CharsetDetector {
		final UniversalDetector detector = new UniversalDetector(null);

		CharsetDetector() {
		}

		public String charsetOf(byte[] bytes) {
			detector.handleData(bytes, 0, bytes.length);
			detector.dataEnd();
			String encoding = detector.getDetectedCharset();
			detector.reset();
			return encoding == null ? "ASCII" : encoding;
		}
	}
}
