package ch.unibe.scg.cc.mappers;

import java.io.IOException;
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

class OccurrenceLoaderProvider implements Provider<LoadingCache<byte[], Iterable<Occurrence>>> {
	final HTable table;
	final OccurrenceFactory occurrenceFactory;

	@Inject
	OccurrenceLoaderProvider(HTable table, OccurrenceFactory occurrenceFactory) {
		this.table = table;
		this.occurrenceFactory = occurrenceFactory;
	}

	static interface OccurrenceFactory {
		public Occurrence make(Result result, byte[] hash);
	}

	static class File2FunctionFactory implements OccurrenceFactory {
		@Override
		public Occurrence make(Result result, byte[] hash) {
			return Occurrence.newBuilder().setFunctionHash(ByteString.copyFrom(hash))
					.setFileHash(ByteString.copyFrom(result.getRow())).setFunctionBaseline(Bytes.toInt(result.value()))
					.build();
		}
	}

	static class Version2FileFactory implements OccurrenceFactory {
		@Override
		public Occurrence make(Result result, byte[] hash) {
			return Occurrence.newBuilder().setVersionHash(ByteString.copyFrom(result.getRow()))
					.setFileNameHash(ByteString.copyFrom(result.value())).build();
		}
	}

	static class Project2VersionFactory implements OccurrenceFactory {
		@Override
		public Occurrence make(Result result, byte[] hash) {
			return Occurrence.newBuilder().setProjectNameHash(ByteString.copyFrom(result.getRow()))
					.setTag(Bytes.toString(result.value())).build();
		}
	}

	@Override
	public LoadingCache<byte[], Iterable<Occurrence>> get() {
		return CacheBuilder.newBuilder().maximumSize(10000).concurrencyLevel(1)
				.build(new CacheLoader<byte[], Iterable<Occurrence>>() {
					@Override
					public Iterable<Occurrence> load(byte[] hash) throws IOException {
						Collection<Occurrence> ret = Lists.newArrayList();
						Scan scan = new Scan();
						scan.addColumn(Constants.FAMILY, hash);
						for (Result result : table.getScanner(scan)) {
							ret.add(occurrenceFactory.make(result, hash));
						}
						return ret;
					}
				});
	}
}
