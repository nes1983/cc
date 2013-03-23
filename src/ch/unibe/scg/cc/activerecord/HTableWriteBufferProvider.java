package ch.unibe.scg.cc.activerecord;

import javax.inject.Inject;
import javax.inject.Provider;

import ch.unibe.scg.cc.mappers.HTableWriteBuffer;
import ch.unibe.scg.cc.mappers.HTableWriteBuffer.BufferFactory;

public class HTableWriteBufferProvider implements Provider<HTableWriteBuffer> {
	@Inject
	HTableProvider htableProvider;

	@Inject
	BufferFactory bufferFactory;

	@Override
	public HTableWriteBuffer get() {
		return bufferFactory.create(htableProvider.get());
	}
}
