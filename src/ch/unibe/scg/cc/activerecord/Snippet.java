package ch.unibe.scg.cc.activerecord;

import javax.inject.Inject;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import ch.unibe.scg.cc.StandardHasher;

public class Snippet extends Column {
	private static final byte[] SNIPPET_VALUE = Bytes.toBytes("sv");
	private byte[] hash;
	private String snippet;
	private Project project;
	private Function function;
	private Location location;
	private byte type;

	StandardHasher standardHasher;

	@Inject
	public Snippet(StandardHasher standardHasher) {
		this.standardHasher = standardHasher;
	}

	public void save(Put put) {
		assert location != null;
		int lineNumberRelativeToFunction = location.getFirstLine();
		byte[] locationFirstLine = Bytes.toBytes(lineNumberRelativeToFunction);
		byte[] locationLength = Bytes.toBytes(location.getLength());
		put.add(FAMILY_NAME, getHash(), 0l, Bytes.add(locationFirstLine, locationLength));
	}

	public byte[] getHash() {
		return Bytes.add(new byte[] { type }, hash);
	}

	public void setHash(byte[] hash) {
		assert hash.length == 20;
		this.hash = hash;
	}

	public Project getProject() {
		return project;
	}

	public void setProject(Project project) {
		this.project = project;
	}

	public Location getLocation() {
		return location;
	}

	public void setLocation(Location location) {
		this.location = location;
	}

	public Function getFunction() {
		return function;
	}

	public void setFunction(Function function) {
		this.function = function;
	}

	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}

	public void saveSnippet(Put put) {
		put.add(FAMILY_NAME, SNIPPET_VALUE, 0l, Bytes.toBytes(snippet));
	}

	public String getSnippet() {
		return this.snippet;
	}

	public void setSnippet(String snippet) {
		this.snippet = snippet;
	}
}
