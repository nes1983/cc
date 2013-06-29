package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;

import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.Protos.Occurrence;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
			return Occurrence.newBuilder()
					.setFunctionHash(ByteString.copyFrom(hash))
					.setFileHash(ByteString.copyFrom(result.getRow()))
					.setFunctionBaseline(Bytes.toInt(result.value())).build();
		}
	}

	static class Version2FileFactory implements OccurrenceFactory {
		@Override
		public Occurrence make(Result result, ByteBuffer hash) {
			return Occurrence.newBuilder()
					.setVersionHash(ByteString.copyFrom(result.getRow()))
					.setFileNameHash(ByteString.copyFrom(result.value())).build();
		}
	}

	static class Project2VersionFactory implements OccurrenceFactory {
		@Override
		public Occurrence make(Result result, ByteBuffer hash) {
			return Occurrence.newBuilder()
					.setProjectNameHash(ByteString.copyFrom(result.getRow()))
					.setTag(Bytes.toString(result.value())).build();
		}
	}

	@Override
	public LoadingCache<ByteBuffer, Iterable<Occurrence>> get() {
		return CacheBuilder.newBuilder().maximumSize(500).concurrencyLevel(1)
				.build(new CacheLoader<ByteBuffer, Iterable<Occurrence>>() {
					@Override
					public Iterable<Occurrence> load(ByteBuffer hash) throws IOException {
						Collection<Occurrence> ret = new ArrayList<>();
						Get indexLookup = new Get(Bytes.getBytes(hash));
						indexLookup.addFamily(Constants.INDEX_FAMILY);
						for(Entry<byte[], byte[]> column :
								table.get(indexLookup).getFamilyMap(Constants.INDEX_FAMILY).entrySet()) {
							Get get = new Get(column.getKey());
							get.addFamily(Constants.FAMILY);
							ret.add(occurrenceFactory.make(table.get(get), hash));
						}
						return ret;
					}
				});
	}
}
