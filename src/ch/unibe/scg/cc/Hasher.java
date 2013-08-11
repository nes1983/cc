package ch.unibe.scg.cc;

import java.io.Serializable;

interface Hasher extends Serializable {
	public byte[] hash(String document) throws CannotBeHashedException;
}
