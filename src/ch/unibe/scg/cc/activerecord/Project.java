package ch.unibe.scg.cc.activerecord;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.StandardHasher;

import com.google.inject.assistedinject.Assisted;

public class Project extends Column implements IColumn {

	private static final byte[] PROJECT_NAME = Bytes.toBytes("pn");
	private final String name;
	private final Version version;
	private final String tag;
	private final byte[] hash;
	private final IPutFactory putFactory;

	@Inject
	public Project(StandardHasher standardHasher, @Assisted("name") String name, @Assisted Version version,
			@Assisted("tag") String tag, IPutFactory putFactory) {
		this.name = name;
		this.version = version;
		this.tag = tag;
		this.hash = standardHasher.hash(getName());
		this.putFactory = putFactory;
	}

	public void save(Put put) throws IOException {
		put.add(FAMILY_NAME, version.getHash(), 0l, Bytes.toBytes(tag));

		Put s = putFactory.create(this.hash);
		s.add(FAMILY_NAME, PROJECT_NAME, 0l, Bytes.toBytes(getName()));
		strings.put(s);
	}

	@Override
	public byte[] getHash() {
		return this.hash;
	}

	public String getName() {
		return name;
	}

	public Version getVersion() {
		return version;
	}

	public String getTag() {
		return tag;
	}
}
