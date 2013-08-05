package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.MRJobConfig;

import ch.unibe.scg.cc.Protos.Clone;
import ch.unibe.scg.cc.Protos.CloneOrBuilder;
import ch.unibe.scg.cc.Protos.Snippet;

import com.google.common.base.Optional;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;

/** See paper. */
public class MakeSnippet2Function implements Runnable {
	static Logger logger = Logger.getLogger(MakeSnippet2Function.class.getName());
	private final HTable snippet2Function;
	private final MapReduceLauncher launcher;
	private final Provider<Scan> scanProvider;

	@Inject
	MakeSnippet2Function(@Named("snippet2function") HTable snippet2Function, MapReduceLauncher launcher,
			Provider<Scan> scanProvider) {
		this.snippet2Function = snippet2Function;
		this.launcher = launcher;
		this.scanProvider = scanProvider;
	}

	static class Function2RoughClonesCodec {
		static byte[] encodeColumnKey(CloneOrBuilder m) {
			return Bytes.add(m.getThatSnippet().getFunction().toByteArray(),
					Bytes.toBytes(m.getThisSnippet().getPosition()),
					Bytes.toBytes(m.getThisSnippet().getLength()));
		}
	}


	static class MakeSnippet2FunctionMapper extends GuiceTableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		/** receives rows from htable function2snippet */
		// super class uses unchecked types
		@Override
		public void map(ImmutableBytesWritable functionHashKey, Result value, Context context) throws IOException,
				InterruptedException {
			byte[] functionHash = functionHashKey.get();
			assert functionHash.length == 20;

			logger.finer("map " + BaseEncoding.base16().encode(functionHashKey.get()).substring(0, 4));

			NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(Constants.FAMILY);

			for (Entry<byte[], byte[]> column : familyMap.entrySet()) {
				byte[] snippet = column.getKey();
				byte[] functionHashPlusLocation = Bytes.add(functionHash, column.getValue());

				logger.finer("snippet " + BaseEncoding.base16().encode(snippet).substring(0, 6) + " found");

				context.write(new ImmutableBytesWritable(snippet), new ImmutableBytesWritable(functionHashPlusLocation));
			}
		}
	}

	static class MakeSnippet2FunctionReducer extends
			GuiceTableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		private static final int POPULAR_SNIPPET_THRESHOLD = 500;
		final private PutFactory putFactory;

		@Inject
		public MakeSnippet2FunctionReducer(@Named("snippet2function") HTableWriteBuffer snippet2Function,
				PutFactory putFactory) {
			super(snippet2Function);
			this.putFactory = putFactory;
		}

		@Override
		public void reduce(final ImmutableBytesWritable snippetKey,
				Iterable<ImmutableBytesWritable> functionHashesPlusLocations, Context context) throws IOException,
				InterruptedException {
			logger.finer("reduce " + BaseEncoding.base16().encode(snippetKey.get()).substring(0, 6));

			Collection<Snippet> snippets = new ArrayList<>();
			for (ImmutableBytesWritable in : functionHashesPlusLocations) {
				snippets.add(Snippet.newBuilder().setFunction(ByteString.copyFrom(Bytes.head(in.get(), 20)))
						.setPosition(Bytes.toInt(Bytes.head(in.get(), 4)))
						.setLength(Bytes.toInt(Bytes.tail(in.get(), 4)))
						.setHash(ByteString.copyFrom(snippetKey.get())).build());
			}

			if (snippets.size() <= 1) {
				return; // prevent processing non-recurring hashes
			}

			// special handling of popular snippets
			if (snippets.size() > POPULAR_SNIPPET_THRESHOLD) {
				// fill popularSnippets table
				for (Snippet snippet : snippets) {
					Put put = putFactory.create(PopularSnippetsCodec.encodeRowKey(snippet));
					put.add(Constants.FAMILY, PopularSnippetsCodec.encodeColumnKey(snippet), 0l,
							PopularSnippetsCodec.encodeColumnKey(snippet));
					write(put);
				}
				// we're done, don't go any further!
				return;
			}

			for (Snippet thisSnippet : snippets) {
				for (Snippet thatSnippet : snippets) {
					// save only half of the functions as row-key
					// full table gets reconstructed in MakeSnippet2FineClones
					// This *must* be the same as in CloneExpander.
					if (thisSnippet.getFunction().asReadOnlyByteBuffer()
							.compareTo(thatSnippet.getFunction().asReadOnlyByteBuffer()) >= 0) {
						continue;
					}

					//	REMARK 1: we don't set thisFunction because it gets
					//	already passed to the reducer as key. REMARK 2: we don't
					//	set thatSnippet because it gets already stored in
					//	thisSnippet
					Clone clone = Clone.newBuilder().setThisSnippet(thisSnippet)
							.setThatSnippet(thatSnippet).build();

					byte[] columnKey = Function2RoughClonesCodec.encodeColumnKey(clone);
					Put put = putFactory.create(thisSnippet.getFunction().toByteArray());
					put.add(Constants.FAMILY, columnKey, 0l, clone.toByteArray());
					context.write(new ImmutableBytesWritable(thisSnippet.getFunction().toByteArray()), put);
				}
			}
		}
	}

	@Override
	public void run() {
		try {
			launcher.truncate(snippet2Function);

			Configuration config = new Configuration();
			config.set(MRJobConfig.MAP_LOG_LEVEL, "DEBUG");
			config.set(MRJobConfig.NUM_REDUCES, "30");
			// TODO test that
			config.set(MRJobConfig.REDUCE_MERGE_INMEM_THRESHOLD, "0");
			config.set(MRJobConfig.REDUCE_MEMTOMEM_ENABLED, "true");
			config.set(MRJobConfig.IO_SORT_MB, "256");
			config.set(MRJobConfig.IO_SORT_FACTOR, "100");
			config.set(MRJobConfig.JOB_UBERTASK_ENABLE, "true");
			// set to 1 if unsure TODO: check max mem allocation if only 1 jvm
			config.set(MRJobConfig.JVM_NUMTASKS_TORUN, "-1");
			config.set(MRJobConfig.TASK_TIMEOUT, "86400000");
			config.set(MRJobConfig.MAP_MEMORY_MB, "1536");
			config.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx1024M");
			config.set(MRJobConfig.REDUCE_MEMORY_MB, "3072");
			config.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx2560M");
			config.set(Constants.GUICE_CUSTOM_MODULES_ANNOTATION_STRING, HBaseModule.class.getName());

			Scan scan = scanProvider.get();
			scan.addFamily(Constants.FAMILY);

			launcher.launchMapReduceJob(MakeSnippet2Function.class.getName() + "Job", config,
					Optional.of("function2snippet"), Optional.of("snippet2function"), Optional.of(scan),
					MakeSnippet2FunctionMapper.class.getName(),
					Optional.of(MakeSnippet2FunctionReducer.class.getName()), ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);
		} catch (IOException | ClassNotFoundException e) {
			throw new WrappedRuntimeException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return; // Exit.
		}
	}
}
