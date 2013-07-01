package ch.unibe.scg.cc.mappers;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.inject.Named;
import javax.inject.Provider;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.BaseEncoding;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

/** key is a functionhash, value is the complete functionstring */
class CloneLoaderProvider implements Provider<LoadingCache<byte[], String>> {
	@Inject(optional = true)
	@Named("strings")
	HTable strings;

	@Override
	public LoadingCache<byte[], String> get() {
		return CacheBuilder.newBuilder().maximumSize(10000).concurrencyLevel(1)
				.build(new CacheLoader<byte[], String>() {
					@Override
					public String load(byte[] functionHash) throws IOException {
						Result result = strings.get(new Get(functionHash));
						if (result == null) {
							throw new RuntimeException("String for function "
									+ BaseEncoding.base16().encode(functionHash) + " not found");
						}
						return Bytes.toString(result.getBytes().get());
					}
				});
	}

	static class ClonedStrings {
		final String thisString, thatString;

		public ClonedStrings(String thisString, String thatString) {
			this.thisString = thisString;
			this.thatString = thatString;
		}
	}

	@BindingAnnotation
	@Target({ FIELD, PARAMETER, METHOD })
	@Retention(RUNTIME)
	public static @interface CloneLoader {
	}
}
