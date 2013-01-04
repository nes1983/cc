package ch.unibe.scg.cc.git;

import org.eclipse.jgit.lib.ObjectId;

public class PackedRef {

	final ObjectId key;
	final String name;

	PackedRef(ObjectId key, String name) {
		this.key = key;
		this.name = name;
	}

	public ObjectId getKey() {
		return key;
	}

	public String getName() {
		return name;
	}

}
