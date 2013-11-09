package ch.unibe.scg.cc;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.Test;

import ch.unibe.scg.cc.GitPopulator.PackedRef;
import ch.unibe.scg.cc.GitPopulator.PackedRefParser;
import ch.unibe.scg.cc.Protos.GitRepo;
import ch.unibe.scg.cells.Cell;
import ch.unibe.scg.cells.Codec;
import ch.unibe.scg.cells.Mapper;
import ch.unibe.scg.cells.OneShotIterable;
import ch.unibe.scg.cells.Sink;
import ch.unibe.scg.cells.hadoop.HadoopPipeline;
import ch.unibe.scg.cells.hadoop.Table;
import ch.unibe.scg.cells.hadoop.TableAdmin;
import ch.unibe.scg.cells.hadoop.UnibeModule;

import com.google.common.primitives.Ints;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.ByteString;

@SuppressWarnings("javadoc")
public final class GitInputFormatTest {
	private static class RefCountCodec implements Codec<RefCount> {
		private static final long serialVersionUID = 1L;

		@Override
		public Cell<RefCount> encode(RefCount i) {
			return Cell.make(ByteString.copyFromUtf8(i.name),
					ByteString.copyFrom(Ints.toByteArray(i.count)),
					ByteString.EMPTY);
		}

		@Override
		public RefCount decode(Cell<RefCount> encoded) throws IOException {
			return new RefCount(
				Ints.fromByteArray(encoded.getColumnKey().toByteArray()),
				encoded.getRowKey().toStringUtf8());
		}
	}

	private static class RefCount {
		final int count;
		final String name;

		RefCount(int count, String name) {
			this.count = count;
			this.name = name;
		}
	}

	private static class PackRefCounter implements Mapper<GitRepo, RefCount> {
		private static final long serialVersionUID = 1L;

		@Override
		public void close() throws IOException { }

		@Override
		public void map(GitRepo first, OneShotIterable<GitRepo> row, Sink<RefCount> sink) throws IOException,
				InterruptedException {
			for (GitRepo repo : row) {
				List<PackedRef> tags = new PackedRefParser().parse(repo.getPackRefs().newInput());
				sink.write(new RefCount(tags.size(), repo.getProjectName()));
			}
		}
	}

	@Test
	public void test() throws IOException, InterruptedException {
		Injector i = Guice.createInjector(new UnibeModule());

		Configuration conf = i.getInstance(Configuration.class);

		conf.set(MRJobConfig.MAP_MEMORY_MB, "4000");
		conf.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx3300m");

		try(Table<RefCount> tab
				= i.getInstance(TableAdmin.class).createTemporaryTable(ByteString.copyFromUtf8("f"))) {
			HadoopPipeline<GitRepo, RefCount> pipe = HadoopPipeline.fromHadoopToTable(conf,
					GitInputFormat.class,
					new Path("har://hdfs-haddock.unibe.ch:/projects/datasetbackup.har/repos/"),
					tab);
			pipe
				.influx(new GitInputFormat.GitRepoCodec())
				.mapAndEfflux(new PackRefCounter(), new RefCountCodec());
		}
	}
}
