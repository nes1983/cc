package ch.unibe.scg.cc;

public interface Hasher {
	public byte[] hash(String document) throws CannotBeHashedException;
}
