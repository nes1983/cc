package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.MRJobConfig;

import ch.unibe.scg.cc.ByteUtils;
import ch.unibe.scg.cc.Protos.SnippetLocation;
import ch.unibe.scg.cc.Protos.SnippetMatch;
import ch.unibe.scg.cc.WrappedRuntimeException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class MakeFunction2FineClones implements Runnable {
	static Logger logger = Logger.getLogger(MakeFunction2FineClones.class.getName());
	final HTable function2fineclones;
	final HTable popularSnippets;
	final MRWrapper mrWrapper;
	private Map<ByteBuffer, SnippetLocation> popularSnippetsMap;

	@Inject
	MakeFunction2FineClones(MRWrapper mrWrapper, @Named("popularSnippets") HTable popularSnippets,
			@Named("function2fineclones") HTable function2fineclones) throws IOException {
		this.mrWrapper = mrWrapper;
		this.function2fineclones = function2fineclones;
		this.popularSnippets = popularSnippets;

		fillPopularSnippetsMap(); // TODO Move into Provider.
	}

	private void fillPopularSnippetsMap() throws IOException {
		Scan scan = new Scan();
		// TODO play with caching. (100 is the default value)
		scan.setCacheBlocks(false);
		scan.addFamily(GuiceResource.FAMILY);
		ResultScanner rs = popularSnippets.getScanner(scan);
		popularSnippetsMap = new HashMap<ByteBuffer, SnippetLocation>();
		for (Result r : rs) {
			ByteBuffer function = ByteBuffer.wrap(r.getRow());
			NavigableMap<byte[], byte[]> fm = r.getFamilyMap(GuiceResource.FAMILY);
			Set<Entry<byte[], byte[]>> columns = fm.entrySet();
			for (Entry<byte[], byte[]> column : columns) {
				// we don't set to function because it gets stored in the key
				SnippetLocation snippetLocation = SnippetLocation.newBuilder()
						.setSnippet(ByteString.copyFrom(column.getKey()))
						.setPosition(Bytes.toInt(Bytes.head(column.getValue(), 4)))
						.setLength(Bytes.toInt(Bytes.tail(column.getValue(), 4))).build();
				popularSnippetsMap.put(function, snippetLocation);
			}
		}
	}

	public class MakeFunction2FineClonesMapper extends GuiceTableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
		/** receives rows from htable function2roughclones */
		@Override
		public void map(ImmutableBytesWritable uselessKey, Result value,
				@SuppressWarnings("rawtypes") org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException,
				InterruptedException {
			final byte[] function = value.getRow();
			assert function.length == 20;

			logger.finer("map function " + ByteUtils.bytesToHex(function));

			NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(GuiceResource.FAMILY);
			Set<Entry<byte[], byte[]>> columns = familyMap.entrySet();
			Iterable<SnippetMatch> matches = Iterables.transform(columns,
					new Function<Entry<byte[], byte[]>, SnippetMatch>() {
						public SnippetMatch apply(Entry<byte[], byte[]> cell) {
							// extract information from cellKey
							final ByteString thatFunction = ByteString.copyFrom(Bytes.head(cell.getKey(), 21));
							final int thisPosition = Bytes.toInt(Bytes.head(Bytes.tail(cell.getKey(), 8), 4));
							final int thisLength = Bytes.toInt(Bytes.tail(cell.getKey(), 4));

							// now reconstruct full SnippetLocations
							try {
								final SnippetMatch partialSnippetMatch = SnippetMatch.parseFrom(cell.getValue());
								return SnippetMatch
										.newBuilder(partialSnippetMatch)
										.setThisSnippetLocation(
												SnippetLocation
														.newBuilder(partialSnippetMatch.getThisSnippetLocation())
														.setFunction(ByteString.copyFrom(function))
														.setPosition(thisPosition).setLength(thisLength))
										.setThatSnippetLocation(
												SnippetLocation
														.newBuilder(partialSnippetMatch.getThatSnippetLocation())
														.setFunction(thatFunction)).build();
							} catch (InvalidProtocolBufferException e) {
								throw new WrappedRuntimeException(e);
							}
						}
					});
			// TODO matching (symmetrical - so do only half of it in the
			// matching part)
			// TODO after matching expand to full clones
		}
	}

	public static class MakeFunction2FineClonesReducer extends
			GuiceTableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
		@Inject
		public MakeFunction2FineClonesReducer(@Named("function2fineclones") HTableWriteBuffer function2fineclones) {
			super(function2fineclones);
		}

		@Override
		public void reduce(ImmutableBytesWritable functionHashKey,
				Iterable<ImmutableBytesWritable> snippetPlusLocationValues, Context context) throws IOException,
				InterruptedException {
			// TODO
		}
	}

	@Override
	public void run() {
		try {
			mrWrapper.truncate(function2fineclones);

			Scan scan = new Scan();
			scan.setCaching(100); // TODO play with this. (100 is default value)
			scan.setCacheBlocks(false);
			scan.addFamily(GuiceResource.FAMILY); // Gets all columns from the
													// specified family.

			Configuration config = new Configuration();
			config.set(MRJobConfig.MAP_LOG_LEVEL, "DEBUG");
			config.set(MRJobConfig.NUM_REDUCES, "30");
			// TODO test that
			config.set(MRJobConfig.REDUCE_MERGE_INMEM_THRESHOLD, "0");
			config.set(MRJobConfig.REDUCE_MEMTOMEM_ENABLED, "true");
			config.set(MRJobConfig.IO_SORT_MB, "512");
			config.set(MRJobConfig.IO_SORT_FACTOR, "100");
			config.set(MRJobConfig.JOB_UBERTASK_ENABLE, "true");
			// set to 1 if unsure TODO: check max mem allocation if only 1 jvm
			config.set(MRJobConfig.JVM_NUMTASKS_TORUN, "-1");
			config.set(MRJobConfig.TASK_TIMEOUT, "86400000");
			config.set(MRJobConfig.MAP_MEMORY_MB, "1536");
			config.set(MRJobConfig.MAP_JAVA_OPTS, "-Xmx1024M");
			config.set(MRJobConfig.REDUCE_MEMORY_MB, "3072");
			config.set(MRJobConfig.REDUCE_JAVA_OPTS, "-Xmx2560M");

			mrWrapper.launchMapReduceJob(MakeFunction2FineClones.class.getName() + "Job", config,
					Optional.of("function2roughclones"), Optional.of("function2fineclones"), scan,
					MakeFunction2FineClonesMapper.class.getName(),
					Optional.of(MakeFunction2FineClonesReducer.class.getName()), ImmutableBytesWritable.class,
					ImmutableBytesWritable.class);
		} catch (IOException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (InterruptedException e) {
			throw new WrappedRuntimeException(e.getCause());
		} catch (ClassNotFoundException e) {
			throw new WrappedRuntimeException(e.getCause());
		}
	}
}
