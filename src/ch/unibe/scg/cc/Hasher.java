package ch.unibe.scg.cc;

interface Hasher {
	public byte[] hash(String document) throws CannotBeHashedException;
}
