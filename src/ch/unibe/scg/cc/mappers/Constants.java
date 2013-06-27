package ch.unibe.scg.cc.mappers;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;

public class Constants {
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
	/** used as standard */
	public static final byte[] FAMILY = Bytes.toBytes("d");
	/** used for index */
	public static final byte[] INDEX_FAMILY = Bytes.toBytes("i");
	public static final String GUICE_MAPPER_ANNOTATION_STRING = "GuiceMapperAnnotation";
	public static final String GUICE_REDUCER_ANNOTATION_STRING = "GuiceReducerAnnotation";
	public static final String GUICE_CUSTOM_MODULE_ANNOTATION_STRING = "GuiceCustomModuleAnnotation";
	/**
	 * String values of counters should be kept synchronized with
	 * {@link org.apache.hadoop.mapreduce.Counters}
	 */
	public static final String COUNTER_CANNOT_BE_HASHED = "CANNOT_BE_HASHED";
	public static final String COUNTER_SUCCESSFULLY_HASHED = "SUCCESSFULLY_HASHED";
	public static final String COUNTER_PROCESSED_FILES = "PROCESSED_FILES";
	public static final String COUNTER_IGNORED_FILES = "IGNORED_FILES";
	public static final String COUNTER_MAKE_FUNCTION_2_FINE_CLONES_ARRAY_EXCEPTIONS = "MAKE_FUNCTION_2_FINE_CLONES_ARRAY_EXCEPTIONS";
	public static final String COUNTER_FUNCTIONS = "FUNCTIONS";
	public static final String COUNTER_LOC = "LOC";
	public static final String COUNTER_CLONES_REJECTED = "CLONES_REJECTED";
	public static final String COUNTER_CLONES_PASSED = "CLONES_PASSED";
}
