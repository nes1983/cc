package ch.unibe.scg.cc.mappers;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
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

import ch.unibe.scg.cc.Protos.SnippetLocation;
import ch.unibe.scg.cc.Protos.SnippetMatch;
import ch.unibe.scg.cc.WrappedRuntimeException;
import ch.unibe.scg.cc.activerecord.Column;
import ch.unibe.scg.cc.activerecord.PutFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
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

	static class ColumnKeyConverter {
		static final int THAT_FUNCTION_LENGTH = 20;
		static final int THIS_POSITION_LENGTH = 4;
		static final int THIS_LENGTH = 4;

		static byte[] encode(byte[] thatFunction, int thisPosition, int thisLength) {
			checkArgument(thatFunction.length == THAT_FUNCTION_LENGTH, "function length illegally was "
					+ thatFunction.length);

			return Bytes.add(thatFunction, Bytes.toBytes(thisPosition), Bytes.toBytes(thisLength));
		}

		static ColumnKey2 decode(byte[] encoded) {
			return new ColumnKey2(Bytes.head(encoded, THAT_FUNCTION_LENGTH), Bytes.toInt(Bytes.head(
					Bytes.tail(encoded, THIS_POSITION_LENGTH + THIS_LENGTH), THIS_POSITION_LENGTH)), Bytes.toInt(Bytes
					.tail(encoded, THIS_LENGTH)));
		}
	}

	static class ColumnKey2 {
		final byte[] thatFunction;
		final int thisPosition;
		final int thisLength;

		ColumnKey2(byte[] thatFunction, int thisPosition, int thisLength) {
			this.thatFunction = thatFunction;
			this.thisPosition = thisPosition;
			this.thisLength = thisLength;
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

			NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(Column.FAMILY_NAME);

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
		public void reduce(ImmutableBytesWritable snippetKey,
				Iterable<ImmutableBytesWritable> functionHashesPlusLocations, Context context) throws IOException,
				InterruptedException {
			logger.finer("reduce " + BaseEncoding.base16().encode(snippetKey.get()).substring(0, 6));

			int functionCount = Iterables.size(functionHashesPlusLocations);
			if (functionCount <= 1) {
				return; // prevent processing non-recurring hashes
			}

			// special handling of popular snippets
			if (functionCount > POPULAR_SNIPPET_THRESHOLD) {
				// fill popularSnippets table
				for (ImmutableBytesWritable functionHashPlusLocation : functionHashesPlusLocations) {
					// TODO: Decoding should not happen here.

					byte[] functionHashPlusLocationKey = functionHashPlusLocation.get();
					byte[] functionHash = Bytes.head(functionHashPlusLocationKey, 20);
					Bytes.tail(functionHashPlusLocationKey, 8);

					SnippetLocation loc = SnippetLocation.newBuilder()
							.setFunction(ByteString.copyFrom(functionHash))
							.setPosition(Bytes.toInt(Bytes.head(functionHashPlusLocationKey, 4)))
							.setLength(Bytes.toInt(Bytes.tail(functionHashPlusLocationKey, 4)))
							.setSnippet(ByteString.copyFrom(snippetKey.get())).build();

					Put put = putFactory.create(PopularSnippetsCodec.encodeRowKey(loc));
					put.add(Constants.FAMILY, PopularSnippetsCodec.encodeColumnKey(loc), 0l,
							PopularSnippetsCodec.encodeColumnKey(loc));
					write(put);
				}
				// we're done, don't go any further!
				return;
			}

			// cross product of the columns of the snippetHash
			for (ImmutableBytesWritable thisFunctionHashPlusLocation : functionHashesPlusLocations) {
				// TODO: Decoding should not happen here.
				// TODO: de-duplicate.
				byte[] functionHashPlusLocationKey = thisFunctionHashPlusLocation.get();
				byte[] functionHash = Bytes.head(functionHashPlusLocationKey, 20);
				byte[] factRelativeLocation = Bytes.tail(functionHashPlusLocationKey, 8);


				byte[] thisFunction = functionHash;
				byte[] thisLocation = factRelativeLocation;
				for (ImmutableBytesWritable thatFunctionHashPlusLocation : functionHashesPlusLocations) {
					// TODO: de-duplicate.
					byte[] thatFunctionHashPlusLocationKey = thatFunctionHashPlusLocation.get();
					byte[] thatFunction = Bytes.head(thatFunctionHashPlusLocationKey, 20);
					byte[] thatLocation = Bytes.tail(thatFunctionHashPlusLocationKey, 8);

					// save only half of the functions as row-key
					// full table gets reconstructed in MakeSnippet2FineClones
					// This *must* be the same as in CloneExpander.
					if (Bytes.compareTo(thisFunction, thatFunction) >= 0) {
						continue;
					}

					/*
					 * REMARK 1: we don't set thisFunction because it gets
					 * already passed to the reducer as key. REMARK 2: we don't
					 * set thatSnippet because it gets already stored in
					 * thisSnippet
					 */
					SnippetMatch snippetMatch = SnippetMatch
							.newBuilder()
							.setThisSnippetLocation(
									SnippetLocation.newBuilder().setPosition(Bytes.toInt(Bytes.head(thisLocation, 4)))
											.setLength(Bytes.toInt(Bytes.tail(thisLocation, 4)))
											.setSnippet(ByteString.copyFrom(snippetKey.get())))
							.setThatSnippetLocation(
									SnippetLocation.newBuilder().setFunction(ByteString.copyFrom(thatFunction))
											.setPosition(Bytes.toInt(Bytes.head(thatLocation, 4)))
											.setLength(Bytes.toInt(Bytes.tail(thatLocation, 4)))).build();

					Put put = putFactory.create(functionHash);

					SnippetLocation thisSnippet = snippetMatch.getThisSnippetLocation();
					SnippetLocation thatSnippet = snippetMatch.getThatSnippetLocation();

					// create partial SnippetLocations and clear fields already set
					// in the cellName to save space in HBase
					SnippetLocation thisPartialSnippet = SnippetLocation.newBuilder(thisSnippet).clearPosition()
							.clearLength().build();
					SnippetLocation thatPartialSnippet = SnippetLocation.newBuilder(thatSnippet).clearFunction()
							.build();
					SnippetMatch partialSnippetMatch = SnippetMatch.newBuilder()
							.setThisSnippetLocation(thisPartialSnippet).setThatSnippetLocation(thatPartialSnippet)
							.build();

					byte[] columnKey = ColumnKeyConverter.encode(thatSnippet.getFunction().toByteArray(),
							thisSnippet.getPosition(), thisSnippet.getLength());
					put.add(Constants.FAMILY, columnKey, 0l, partialSnippetMatch.toByteArray());
					context.write(new ImmutableBytesWritable(thisFunction), put);
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
