package ch.unibe.scg.cc.mappers;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;

/**
 * provides all useful resources to the mapper and reducers
 * 
 * @author vs
 * 
 */
public class GuiceResource {
	public enum ColumnName {
		COUNT_VERSIONS(Bytes.toBytes("nve")), VALUES_VERSIONS(Bytes.toBytes("vve")), COUNT_FILES(Bytes.toBytes("nfi")), VALUES_FILES(
				Bytes.toBytes("vfi")), COUNT_FUNCTIONS(Bytes.toBytes("vfn")), VALUES_FUNCTIONS(Bytes.toBytes("vfn")), COUNT_SNIPPETS(
				Bytes.toBytes("nfa")), VALUES_SNIPPETS(Bytes.toBytes("vfa"));

		private final byte[] name;

		private ColumnName(byte[] name) {
			this.name = name;
		}

		public byte[] getName() {
			return this.name;
		}
	}

	public static final BytesWritable EMPTY_BYTES_WRITABLE = new BytesWritable(new byte[] {});
	public static final byte[] FAMILY = Bytes.toBytes("d");
	public static final String GUICE_MAPPER_ANNOTATION_STRING = "GuiceMapperAnnotation";
	public static final String GUICE_REDUCER_ANNOTATION_STRING = "GuiceReducerAnnotation";

	/**
	 * String values of counters should be kept synchronized with
	 * {@link Counters}
	 */
	public static final String COUNTER_CANNOT_BE_HASHED = "CANNOT_BE_HASHED";
	public static final String COUNTER_SUCCESSFULLY_HASHED = "SUCCESSFULLY_HASHED";
}
