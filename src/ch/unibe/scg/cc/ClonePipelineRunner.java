package ch.unibe.scg.cc;

import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Provider;

import org.unibe.scg.cells.Codec;
import org.unibe.scg.cells.Pipeline;

import ch.unibe.scg.cc.Annotations.Function2FineClones;
import ch.unibe.scg.cc.Annotations.Function2RoughClones;
import ch.unibe.scg.cc.Annotations.Snippet2Functions;
import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.CloneGroup;
import ch.unibe.scg.cc.Protos.GitRepo;
import ch.unibe.scg.cc.Protos.Snippet;

/** Run the clone detector. */
public class ClonePipelineRunner {
	final private Provider<GitPopulator> gitPopulator;
	final private Provider<Function2RoughCloner> function2RoughCloner;
	final private Provider<Function2FineCloner> function2FineCloner;
	final private Codec<GitRepo> repoCodec;
	final private Codec<Snippet> snippet2FunctionsCodec;
	final private Codec<Clone> function2RoughClonesCodec;
	final private Codec<CloneGroup> function2FineClonesCodec;

	@Inject
	ClonePipelineRunner(Provider<GitPopulator> gitPopulator, Provider<Function2RoughCloner> function2RoughCloner,
			Provider<Function2FineCloner> function2FineCloner, Codec<GitRepo> repoCodec, @Snippet2Functions Codec<Snippet> snippet2FunctionsCodec,
			@Function2RoughClones Codec<Clone> function2RoughClonesCodec, @Function2FineClones Codec<CloneGroup> function2FineClonesCodec) {
		this.gitPopulator = gitPopulator;
		this.function2RoughCloner = function2RoughCloner;
		this.function2FineCloner = function2FineCloner;
		this.repoCodec = repoCodec;
		this.snippet2FunctionsCodec = snippet2FunctionsCodec;
		this.function2RoughClonesCodec = function2RoughClonesCodec;
		this.function2FineClonesCodec = function2FineClonesCodec;
	}

	/** Run the clone detector. */
	public void run(Pipeline<GitRepo, CloneGroup> pipe) throws IOException {
		pipe
			.influx(repoCodec)
			.mapper(gitPopulator)
			.shuffle(snippet2FunctionsCodec)
			.mapper(function2RoughCloner)
			.shuffle(function2RoughClonesCodec)
			.efflux(function2FineCloner, function2FineClonesCodec);
	}
}
