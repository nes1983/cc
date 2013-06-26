package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.Protos.Occurrence;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

class OccurrenceLoaderProvider implements Provider<LoadingCache<ByteBuffer, Iterable<Occurrence>>> {
	final HTable table;
	final OccurrenceFactory occurrenceFactory;

	@Inject
	OccurrenceLoaderProvider(HTable table, OccurrenceFactory occurrenceFactory) {
		this.table = table;
		this.occurrenceFactory = occurrenceFactory;
	}

	static interface OccurrenceFactory {
		public Occurrence make(Result result, ByteBuffer hash);
	}

	static class File2FunctionFactory implements OccurrenceFactory {
		@Override
		public Occurrence make(Result result, ByteBuffer hash) {
			return Occurrence.newBuilder().setFunctionHash(ByteString.copyFrom(hash))
					.setFileHash(ByteString.copyFrom(result.getRow())).setFunctionBaseline(Bytes.toInt(result.value()))
					.build();
		}
	}

	static class Version2FileFactory implements OccurrenceFactory {
		@Override
		public Occurrence make(Result result, ByteBuffer hash) {
			return Occurrence.newBuilder().setVersionHash(ByteString.copyFrom(result.getRow()))
					.setFileNameHash(ByteString.copyFrom(result.value())).build();
		}
	}

	static class Project2VersionFactory implements OccurrenceFactory {
		@Override
		public Occurrence make(Result result, ByteBuffer hash) {
			return Occurrence.newBuilder().setProjectNameHash(ByteString.copyFrom(result.getRow()))
					.setTag(Bytes.toString(result.value())).build();
		}
	}

	@Override
	public LoadingCache<ByteBuffer, Iterable<Occurrence>> get() {
		return CacheBuilder.newBuilder().maximumSize(500).concurrencyLevel(1)
				.build(new CacheLoader<ByteBuffer, Iterable<Occurrence>>() {
					@Override
					public Iterable<Occurrence> load(ByteBuffer hash) throws IOException {
						Collection<Occurrence> ret = Lists.newArrayList();
						Scan scan = new Scan(); // Don't use ScanProvider as it's optimized for MR jobs.
						scan.setCacheBlocks(true); // Other reducers might request the same, so cache.
						scan.setCaching(1); // We probably don't need adjacent rows. Only get the current.
						scan.addColumn(Constants.FAMILY, Bytes.getBytes(hash));

						for (Result result : table.getScanner(scan)) {
							ret.add(occurrenceFactory.make(result, hash));
						}
						return ret;
					}
				});
	}
}
