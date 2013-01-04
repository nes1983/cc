package ch.unibe.scg.cc.activerecord;

import java.util.List;

public interface CodeFile extends IColumn {
	byte[] getFileContentsHash();

	List<Function> getFunctions();

	void addFunction(Function function);
}
