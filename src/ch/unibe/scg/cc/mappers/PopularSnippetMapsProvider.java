package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.NavigableMap;

import javax.inject.Named;
import javax.inject.Provider;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import ch.unibe.scg.cc.PopularSnippetMaps;
import ch.unibe.scg.cc.Protos.SnippetLocation;
import ch.unibe.scg.cc.WrappedRuntimeException;

import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;

class PopularSnippetMapsProvider implements Provider<PopularSnippetMaps> {
	@Inject(optional = true)
	@Named("popularSnippets")
	HTable popularSnippets;

	final Provider<Scan> scanProvider;

	@Inject
	PopularSnippetMapsProvider(Provider<Scan> scanProvider) {
		this.scanProvider = scanProvider;
	}

	// Must be static to force sharing across all JVMs.
	private static PopularSnippetMaps popularSnippetMaps;

	@Override
	public PopularSnippetMaps get() {
		synchronized (PopularSnippetMaps.class) {
			if (popularSnippetMaps == null) {
				ImmutableMultimap.Builder<ByteBuffer, SnippetLocation> function2PopularSnippets = ImmutableMultimap
						.builder();
				ImmutableMultimap.Builder<ByteBuffer, SnippetLocation> snippet2PopularSnippets = ImmutableMultimap
						.builder();

				Scan scan = scanProvider.get();
				scan.addFamily(Constants.FAMILY);

				try (ResultScanner rs = popularSnippets.getScanner(scan)) {
					for (Result r : rs) {
						NavigableMap<byte[], byte[]> fm = r.getFamilyMap(Constants.FAMILY);
						for (Entry<byte[], byte[]> cell : fm.entrySet()) {
							// To save space we didn't store a protobuffer object.
							// We create the object now with the information we have.
							SnippetLocation snippetLocation = PopularSnippetsCodec
									.decodeSnippetLocation(r.getRow(), cell.getKey(), cell.getValue());
							function2PopularSnippets.put(snippetLocation.getFunction().asReadOnlyByteBuffer(), snippetLocation);
							snippet2PopularSnippets.put(snippetLocation.getSnippet().asReadOnlyByteBuffer(), snippetLocation);
						}
					}
				} catch (IOException e) {
					throw new WrappedRuntimeException("Problem with popularSnippets occured: ", e);
				}

				popularSnippetMaps = new PopularSnippetMaps(function2PopularSnippets.build(),
						snippet2PopularSnippets.build());
			}

			return popularSnippetMaps;
		}
	}
}
