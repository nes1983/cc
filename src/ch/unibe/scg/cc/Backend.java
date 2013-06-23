package ch.unibe.scg.cc;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Named;
import javax.inject.Provider;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;

import ch.unibe.scg.cc.activerecord.CodeFile;
import ch.unibe.scg.cc.activerecord.Function;
import ch.unibe.scg.cc.activerecord.Location;
import ch.unibe.scg.cc.activerecord.Project;
import ch.unibe.scg.cc.activerecord.PutFactory;
import ch.unibe.scg.cc.activerecord.Snippet;
import ch.unibe.scg.cc.activerecord.Version;
import ch.unibe.scg.cc.lines.StringOfLines;
import ch.unibe.scg.cc.lines.StringOfLinesFactory;
import ch.unibe.scg.cc.mappers.Constants;
import ch.unibe.scg.cc.mappers.HTableWriteBuffer;

import com.google.common.cache.CacheBuilder;
import com.google.common.io.Closer;
import com.google.inject.Inject;

public interface Backend extends Closeable {
	public static final int MINIMUM_LINES = 5;
	public static final int MINIMUM_FRAME_SIZE = MINIMUM_LINES;

	public abstract void shingleRegisterFunction(StringOfLines contentsSOL, Function function);

	/**
	 * Registers a standard unit of code, typically a function, that is atomic
	 * in a sense. Lines must have at least 5 lines.
	 */
	public abstract void registerConsecutiveLinesOfCode(StringOfLines stringOfLines, Function function, byte type);

	public abstract void register(CodeFile codeFile);

	public abstract void register(Project project);

	public abstract void register(Version version);

	public static class RegisterClonesBackend implements Closeable, Backend {
		static Logger logger = Logger.getLogger(RegisterClonesBackend.class.getName());
		final StandardHasher standardHasher;
		final ShingleHasher shingleHasher;
		final StringOfLinesFactory stringOfLinesFactory;
		final PutFactory putFactory;
		final Provider<Location> locationProvider;
		final Provider<Snippet> snippetProvider;

		/** We cache whether or not files and functions have been written. */
		private static final int CACHE_SIZE = 1000000;
		/** Functions that were successfully written to DB in this mapper */
		final Map<ByteBuffer, Boolean> writtenFunctions =
				CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).<ByteBuffer, Boolean>build().asMap();
		/** Files that were successfully written to DB in this mapper */
		final Map<ByteBuffer, Boolean> writtenFiles =
				CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).<ByteBuffer, Boolean>build().asMap();

		@Inject(optional = true)
		@Named("project2version")
		HTableWriteBuffer project2version;

		@Inject(optional = true)
		@Named("version2file")
		HTableWriteBuffer version2file;

		@Inject(optional = true)
		@Named("file2function")
		HTableWriteBuffer file2function;

		@Inject(optional = true)
		@Named("function2snippet")
		HTableWriteBuffer function2snippet;

		@Inject(optional = true)
		@Named("strings")
		HTableWriteBuffer strings;

		// Optional because in MRMain, we have an injector that does not set this
		// property, and can't, because it doesn't have the counter available.
		@Inject(optional = true)
		@Named(Constants.COUNTER_CANNOT_BE_HASHED)
		Counter cannotBeHashedCounter;
		@Inject(optional = true)
		@Named(Constants.COUNTER_SUCCESSFULLY_HASHED)
		Counter successfullyHashedCounter;

		@Inject
		RegisterClonesBackend(StandardHasher standardHasher, ShingleHasher shingleHasher,
				StringOfLinesFactory stringOfLinesFactory, PutFactory putFactory, Provider<Location> locationProvider,
				Provider<Snippet> snippetProvider) {
			this.standardHasher = standardHasher;
			this.shingleHasher = shingleHasher;
			this.stringOfLinesFactory = stringOfLinesFactory;
			this.putFactory = putFactory;
			this.locationProvider = locationProvider;
			this.snippetProvider = snippetProvider;
		}

		@Override
		public void shingleRegisterFunction(StringOfLines contentsSOL, Function function) {
			registerConsecutiveLinesOfCode(contentsSOL, function, shingleHasher, Main.TYPE_3_CLONE);
		}

		@Override
		public void registerConsecutiveLinesOfCode(StringOfLines stringOfLines, Function function, byte type) {
			registerConsecutiveLinesOfCode(stringOfLines, function, standardHasher, type);
		}

		void registerConsecutiveLinesOfCode(StringOfLines stringOfLines, Function function, Hasher hasher, byte type) {
			checkArgument(stringOfLines.getNumberOfLines() >= MINIMUM_LINES,
					"Need at least " + MINIMUM_LINES + ", but got " + stringOfLines.getNumberOfLines());

			for (int frameStart = 0; frameStart + MINIMUM_LINES <= stringOfLines.getNumberOfLines(); frameStart++) {
				String snippet = stringOfLines.getLines(frameStart, MINIMUM_LINES);
				this.registerSnippet(snippet, function, frameStart, hasher, type);
			}
		}

		private void registerSnippet(String snippet, Function function, int from, Hasher hasher, byte type) {
			byte[] hash;
			try {
				hash = hasher.hash(snippet);
				successfullyHashedCounter.increment(1);
			} catch (CannotBeHashedException e) {
				cannotBeHashedCounter.increment(1);
				return;
			}
			Location location = locationProvider.get();
			location.setFirstLine(from);
			Snippet s = snippetProvider.get();
			s.setHash(hash);
			s.setSnippet(snippet);
			s.setLocation(location);
			s.setFunction(function);
			s.setType(type);
			function.addSnippet(s);
		}

		@Override
		public void register(CodeFile codeFile) {
			// TODO write codeFile string into strings table.
			if (writtenFiles.containsKey(ByteBuffer.wrap(codeFile.getHash()))) {
				return; // We've dealt with this file.
			}
			writtenFiles.put(ByteBuffer.wrap(codeFile.getHash()), true);

			for (Function function : codeFile.getFunctions()) {
				Put functionPut = putFactory.create(codeFile.getHash());
				try {
					functionPut.add(CodeFile.FAMILY_NAME, function.getHash(), 0l, Bytes.toBytes(function.getBaseLine()));
					file2function.write(functionPut);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
			List<Function> functions = codeFile.getFunctions();
			for (Function function : functions) {
				register(function);
			}
		}

		private void register(Function function) {
			if (writtenFunctions.containsKey(ByteBuffer.wrap(function.getHash()))) {
				return; // This function has been dealt with.
			}
			writtenFunctions.put(ByteBuffer.wrap(function.getHash()), true);

			Put functionString = putFactory.create(function.getHash());
			try {
				function.saveContents(functionString);
				strings.write(functionString);

				for (Snippet snippet : function.getSnippets()) {
					Put put = putFactory.create(function.getHash());
					snippet.save(put);
					function2snippet.write(put);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void register(Project project) {
			Put put = putFactory.create(project.getHash());
			try {
				project.save(put);
				project2version.write(put);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void register(Version version) {
			Put put = putFactory.create(version.getHash());
			try {
				version.save(put);
				version2file.write(put);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void close() throws IOException {
			Closer closer = Closer.create();
			closer.register(version2file);
			closer.register(file2function);
			closer.register(function2snippet);
			closer.register(strings);
			closer.register(project2version);
			closer.close();
		}
	}
}

