package ch.unibe.scg.cc;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.GitRepo;
import ch.unibe.scg.cells.hadoop.HBaseStorage;
import ch.unibe.scg.cells.hadoop.HadoopCounterModule;
import ch.unibe.scg.cells.hadoop.HadoopPipeline;
import ch.unibe.scg.cells.hadoop.Table;
import ch.unibe.scg.cells.hadoop.TableAdmin;
import ch.unibe.scg.cells.hadoop.UnibeModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.ByteString;

/** Run the clone detector on the cluster. */
public final class Main {
	private static final String OUT_TABLE = "ClonsNov";

	/** Run in cluster. */
	public static void main(String[] args) throws IOException, InterruptedException {
		Injector i = Guice.createInjector(new UnibeModule(),
				new CCModule(new HBaseStorage(), new HadoopCounterModule()));

		Configuration conf = i.getInstance(Configuration.class);
		TableAdmin admin = i.getInstance(TableAdmin.class);

		List<String> tabs = Arrays.asList("Snippets", "Functions", "CodeFiles", "Versions",
				"Projects", "FunctionStrings", "PopularSnippets", OUT_TABLE);
		ByteString fam = ByteString.copyFromUtf8("f");
		if (args.length >= 1 && args[0].equals("--recreateTables")) {
			for (String tabName : tabs) {
				admin.deleteTable(tabName);
				admin.createTable(tabName, fam);
			}
		}

		try (Table<Clone> tab = admin.existing(OUT_TABLE, fam)) {
			HadoopPipeline<GitRepo, Clone> pipe = HadoopPipeline.fromHDFSToTable(conf,
					GitInputFormat.class,
					new Path("har://hdfs-haddock.unibe.ch:/projects/dataset-141.har/repos/"),
					tab);
			PipelineRunner runner = i.getInstance(PipelineRunner.class);
			runner.run(pipe);
		}
	}
}
