package ch.unibe.scg.cc;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.MRJobConfig;

import ch.unibe.scg.cc.GitInputFormat.GitRepoCodec;
import ch.unibe.scg.cc.Protos.GitRepo;
import ch.unibe.scg.cc.Protos.Snippet;
import ch.unibe.scg.cells.Codec;
import ch.unibe.scg.cells.Pipeline;
import ch.unibe.scg.cells.hadoop.HBaseStorage;
import ch.unibe.scg.cells.hadoop.HadoopCounterModule;
import ch.unibe.scg.cells.hadoop.HadoopPipeline;
import ch.unibe.scg.cells.hadoop.Table;
import ch.unibe.scg.cells.hadoop.TableAdmin;
import ch.unibe.scg.cells.hadoop.UnibeModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.ByteString;

/**
 * Benchmark testing the first phase of the clone detector on the cluster.
 * Tests configuration settings in different variations apart from our specific configuration.
 * <p>
 * The running times can be checked in the Hadoop web frontend (see Readme).
 */
public final class HadoopConfigurationTest {
	private static final String OUT_TABLE = "tmpSnippets";

	/** Run in cluster. */
	public static void main(String[] args) throws IOException, InterruptedException {
		Injector i = Guice.createInjector(new UnibeModule(),
				new CCModule(new HBaseStorage(), new HadoopCounterModule()));

		Configuration ccConfiguration = i.getInstance(Configuration.class);
		Iterable<Configuration> testConfigurations = makeTestConfigurations(ccConfiguration);

		TableAdmin admin = i.getInstance(TableAdmin.class);

		ByteString fam = ByteString.copyFromUtf8("f");
		admin.deleteTable(OUT_TABLE);
		admin.createTable(OUT_TABLE, fam);

		for (Configuration conf : testConfigurations) {
			try (Table<Snippet> tab = admin.existing(OUT_TABLE, fam)) {
				HadoopPipeline<GitRepo, Snippet> pipe = HadoopPipeline.fromHDFSToTable(conf,
						GitInputFormat.class,
						new Path("har://hdfs-haddock.unibe.ch:/projects/dataset-141.har/repos/"),
						tab);
				HadoopConfigurationTestPipelineRunner runner = i.getInstance(HadoopConfigurationTestPipelineRunner.class);
				runner.run(pipe);
			}
		}
	}

	static class HadoopConfigurationTestPipelineRunner implements Serializable {
		final private static long serialVersionUID = 1L;

		final private GitPopulator gitPopulator;
		final private Codec<GitRepo> repoCodec;
		final private Codec<Snippet> snippet2FunctionsCodec;

		@Inject
		HadoopConfigurationTestPipelineRunner(GitPopulator gitPopulator,
				GitRepoCodec repoCodec,
				Snippet2FunctionsCodec snippet2FunctionsCodec) {
			this.gitPopulator = gitPopulator;
			this.repoCodec = repoCodec;
			this.snippet2FunctionsCodec = snippet2FunctionsCodec;
		}

		/** Run the clone detector. */
		private void run(Pipeline<GitRepo, Snippet> pipe) throws IOException, InterruptedException {
			pipe
				.influx(repoCodec)
				.mapAndEfflux(gitPopulator, snippet2FunctionsCodec);
		}
	}

	private static List<Configuration> makeTestConfigurations(Configuration ccConf) {
		List<Configuration> configurations = new ArrayList<>();
		configurations.addAll(makeCurrentCCConfiguration(new Configuration(ccConf)));
		configurations.addAll(makeMemoryMb(new Configuration(ccConf)));
		configurations.addAll(makeMaxFailuresAndAttempts(new Configuration(ccConf)));
		configurations.addAll(makeSpeculative(new Configuration(ccConf)));
		configurations.addAll(makeSortResources(new Configuration(ccConf)));
		configurations.addAll(makeMapCompression(new Configuration(ccConf)));
		configurations.addAll(makeSpilling(new Configuration(ccConf)));
		configurations.addAll(makeReducers(new Configuration(ccConf)));
		configurations.addAll(makeSlowStart(new Configuration(ccConf)));
		configurations.addAll(makeReduceMemExperimental(new Configuration(ccConf)));
		configurations.addAll(makeShuffleParallelCopies(new Configuration(ccConf)));
		return configurations;
	}

	private static List<Configuration> makeCurrentCCConfiguration(Configuration configuration) {
		Configuration configurationV1 = new Configuration(configuration);
		configurationV1.set(MRJobConfig.JOB_NAME, "Current CC configuration");

		return Arrays.asList(configurationV1);
	}

	private static List<Configuration> makeMemoryMb(Configuration configuration) {
		Configuration configurationV1 = new Configuration(configuration);
		configurationV1.set(MRJobConfig.JOB_NAME, "MemoryMB lower");
		configurationV1.setLong(MRJobConfig.MAP_MEMORY_MB, 3000L);
		configurationV1.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx2600m");
		configurationV1.setLong(MRJobConfig.REDUCE_MEMORY_MB, 3000L);
		configurationV1.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx2600m");

		Configuration configurationV2 = new Configuration(configuration);
		configurationV2.set(MRJobConfig.JOB_NAME, "MemoryMB higher");
		configurationV2.setLong(MRJobConfig.MAP_MEMORY_MB, 5000L);
		configurationV2.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx4400m");
		configurationV2.setLong(MRJobConfig.REDUCE_MEMORY_MB, 5000L);
		configurationV2.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx4400m");

		return Arrays.asList(configurationV1, configurationV2);
	}

	private static List<Configuration> makeMaxFailuresAndAttempts(Configuration configuration) {
		Configuration configurationV1 = new Configuration(configuration);
		configurationV1.set(MRJobConfig.JOB_NAME, "MaxFailuresAndAttempts variation");
		configurationV1.setInt(MRJobConfig.MAP_MAX_ATTEMPTS, 4);
		configurationV1.setInt(MRJobConfig.MAP_FAILURES_MAX_PERCENT, 0);

		return Arrays.asList(configurationV1);
	}

	private static List<Configuration> makeSpeculative(Configuration configuration) {
		Configuration configurationV1 = new Configuration(configuration);
		configurationV1.set(MRJobConfig.JOB_NAME, "Speculative variation");
		configurationV1.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);

		return Arrays.asList(configurationV1);
	}

	private static List<Configuration> makeSortResources(Configuration configuration) {
		Configuration configurationV1 = new Configuration(configuration);
		configurationV1.set(MRJobConfig.JOB_NAME, "IO Sort lower");
		configurationV1.setInt(MRJobConfig.IO_SORT_MB, 350);
		configurationV1.setInt(MRJobConfig.IO_SORT_FACTOR, 35);

		Configuration configurationV2 = new Configuration(configuration);
		configurationV2.set(MRJobConfig.JOB_NAME, "IO Sort higher");
		configurationV2.setInt(MRJobConfig.IO_SORT_MB, 750);
		configurationV2.setInt(MRJobConfig.IO_SORT_FACTOR, 75);

		return Arrays.asList(configurationV1, configurationV2);
	}

	private static List<Configuration> makeMapCompression(Configuration configuration) {
		Configuration configurationV1 = new Configuration(configuration);
		configurationV1.set(MRJobConfig.JOB_NAME, "MapCompression off");
		configurationV1.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, false);

		Configuration configurationV2 = new Configuration(configuration);
		configurationV2.set(MRJobConfig.JOB_NAME, "MapCompression with DefaultCodec");
		configurationV2.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
		configurationV2.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, DefaultCodec.class.getName());

		return Arrays.asList(configurationV1, configurationV2);
	}

	private static List<Configuration> makeSpilling(Configuration configuration) {
		Configuration configurationV1 = new Configuration(configuration);
		configurationV1.set(MRJobConfig.JOB_NAME, "Spilling lower");
		configurationV1.setFloat(MRJobConfig.MAP_SORT_SPILL_PERCENT, 0.6f);

		Configuration configurationV2 = new Configuration(configuration);
		configurationV2.set(MRJobConfig.JOB_NAME, "Spilling higher");
		configurationV2.setFloat(MRJobConfig.MAP_SORT_SPILL_PERCENT, 0.95f);

		Configuration configurationV3 = new Configuration(configuration);
		configurationV3.set(MRJobConfig.JOB_NAME, "Spilling much lower");
		configurationV3.setFloat(MRJobConfig.MAP_SORT_SPILL_PERCENT, 0.1f);

		return Arrays.asList(configurationV1, configurationV2, configurationV3);
	}

	private static List<Configuration> makeReducers(Configuration configuration) {
		Configuration configurationV1 = new Configuration(configuration);
		configurationV1.set(MRJobConfig.JOB_NAME, "Reducers lower");
		configurationV1.setInt(MRJobConfig.NUM_REDUCES, 18);

		Configuration configurationV2 = new Configuration(configuration);
		configurationV2.set(MRJobConfig.JOB_NAME, "Reducers higher");
		configurationV2.setInt(MRJobConfig.NUM_REDUCES, 38);

		return Arrays.asList(configurationV1, configurationV2);
	}

	private static List<Configuration> makeSlowStart(Configuration configuration) {
		Configuration configurationV1 = new Configuration(configuration);
		configurationV1.set(MRJobConfig.JOB_NAME, "SlowStart 0%");
		configurationV1.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 0.0f);

		Configuration configurationV2 = new Configuration(configuration);
		configurationV2.set(MRJobConfig.JOB_NAME, "SlowStart 50%");
		configurationV2.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 0.5f);

		Configuration configurationV3 = new Configuration(configuration);
		configurationV3.set(MRJobConfig.JOB_NAME, "SlowStart 100%");
		configurationV3.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 1.0f);

		return Arrays.asList(configurationV1, configurationV2, configurationV3);
	}

	private static List<Configuration> makeReduceMemExperimental(Configuration configuration) {
		Configuration configurationV1 = new Configuration(configuration);
		configurationV1.set(MRJobConfig.JOB_NAME, "Reduce Mem2Mem enabled, threshold 0");
		configurationV1.setBoolean(MRJobConfig.REDUCE_MEMTOMEM_ENABLED, true);
		configurationV1.setInt(MRJobConfig.REDUCE_MERGE_INMEM_THRESHOLD, 0);

		Configuration configurationV2 = new Configuration(configuration);
		configurationV2.set(MRJobConfig.JOB_NAME, "Reduce Mem2Mem enabled, threshold 1000");
		configurationV2.setBoolean(MRJobConfig.REDUCE_MEMTOMEM_ENABLED, true);
		configurationV2.setInt(MRJobConfig.REDUCE_MERGE_INMEM_THRESHOLD, 1000);

		Configuration configurationV3 = new Configuration(configuration);
		configurationV3.set(MRJobConfig.JOB_NAME, "Reduce Mem2Mem enabled, threshold 3000");
		configurationV3.setBoolean(MRJobConfig.REDUCE_MEMTOMEM_ENABLED, true);
		configurationV3.setInt(MRJobConfig.REDUCE_MERGE_INMEM_THRESHOLD, 3000);

		return Arrays.asList(configurationV1, configurationV2, configurationV3);
	}

	private static List<Configuration> makeShuffleParallelCopies(Configuration configuration) {
		Configuration configurationV1 = new Configuration(configuration);
		configurationV1.set(MRJobConfig.JOB_NAME, "Shuffle Parallel Copies 2");
		configurationV1.setInt(MRJobConfig.SHUFFLE_PARALLEL_COPIES, 2);

		Configuration configurationV2 = new Configuration(configuration);
		configurationV2.set(MRJobConfig.JOB_NAME, "Shuffle Parallel Copies 10");
		configurationV2.setInt(MRJobConfig.SHUFFLE_PARALLEL_COPIES, 10);

		return Arrays.asList(configurationV1, configurationV2);
	}
}
