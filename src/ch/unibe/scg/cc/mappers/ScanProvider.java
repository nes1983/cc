package ch.unibe.scg.cc.mappers;

import javax.inject.Provider;

import org.apache.hadoop.hbase.client.Scan;

class ScanProvider implements Provider<Scan> {
	@Override
	public Scan get() {
		Scan scan = new Scan();
		// HBase book says 500, see chapter 12.9.1. Hbase Book
		// This is 10 times faster than the default value.
		scan.setCaching(1000);

		scan.setCacheBlocks(false); // HBase book 12.9.5. Block Cache
		return scan;
	}
}
