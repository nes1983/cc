package ch.unibe.scg.cc.mappers;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;

/**
 * provides all useful resources to the mapper and reducers
 * @author vs
 *
 */
public class GuiceResource {

	public static final BytesWritable EMPTY_BYTES_WRITABLE = new BytesWritable(new byte[] {});
	public static final byte[] FAMILY = Bytes.toBytes("d");
	public static final byte[] COLUMN_COUNT_PROJECTS = Bytes.toBytes("np");
	public static final byte[] COLUMN_VALUES_PROJECTS = Bytes.toBytes("vp");
	public static final byte[] COLUMN_COUNT_VERSIONS = Bytes.toBytes("nv");
	public static final byte[] COLUMN_VALUES_VERSIONS = Bytes.toBytes("vv");
	public static final byte[] COLUMN_COUNT_FILES = Bytes.toBytes("nf");
	public static final byte[] COLUMN_VALUES_FILES = Bytes.toBytes("vf");
	public static final byte[] COLUMN_COUNT_FUNCTIONS = Bytes.toBytes("nf");
	public static final byte[] COLUMN_VALUES_FUNCTIONS = Bytes.toBytes("vf");
	
	/* ONLY FOR FAST COPY/PASTE...
	final HTable versions;
	final HTable codefiles;
	final HTable functions;
	final HTable strings;
	final HTable hashfactContent;
	final HTable indexProjects;
	final HTable indexFiles2Versions;
	final HTable indexFunctions2Files;
	final HTable indexHashfacts2Functions;
	final HTable indexFactToProject;
	final IPutFactory putFactory;
	final HashSerializer hashSerializer;
	final Provider<Set<byte[]>> byteSetProvider;
	
	@Inject
	public GuiceResource(
			@Named("projects") HTable projects,
			@Named("versions") HTable versions,
			@Named("files") HTable codefiles,
			@Named("functions") HTable functions,
			@Named("strings") HTable strings,
			@Named("hashfactContent") HTable hashfactContent,
			@Named("indexVersions2Projects") HTable indexProjects,
			@Named("indexFiles2Versions") HTable indexFiles2Versions,
			@Named("indexFunctions2Files") HTable indexFunctions2Files,
			@Named("indexHashfacts2Functions") HTable indexHashfacts2Functions,
			@Named("indexFactToProject") HTable indexFactToProject,
			IPutFactory putFactory,
			HashSerializer hashSerializer,
			Provider<Set<byte[]>> byteSetProvider) {
		this.versions = versions;
		this.codefiles = codefiles;
		this.functions = functions;
		this.strings = strings;
		this.hashfactContent = hashfactContent;
		this.indexProjects = indexProjects;
		this.indexFiles2Versions = indexFiles2Versions;
		this.indexFunctions2Files = indexFunctions2Files;
		this.indexHashfacts2Functions = indexHashfacts2Functions;
		this.indexFactToProject = indexFactToProject;
		this.putFactory = putFactory;
		this.hashSerializer = hashSerializer;
		this.byteSetProvider = byteSetProvider;
	}

	Put createPut(byte[] rowKey) {
		return this.putFactory.create(rowKey);
	}
	*/
}
