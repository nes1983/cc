package ch.unibe.scg.cc.mappers;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** A {@link GuiceMapper} that writes into an HTable. */
public abstract class GuiceTableMapper<KEYOUT, VALUEOUT> extends
		GuiceMapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT> {

	private HTableWriteBuffer writeBuffer;

	public GuiceTableMapper() {
	}

	public GuiceTableMapper(HTableWriteBuffer htableWriteBuffer) {
		this();
		this.writeBuffer = htableWriteBuffer;
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		if (this.writeBuffer != null) {
			this.writeBuffer.close();
		}
	}

	protected final void write(Put put) throws IOException {
		if (this.writeBuffer == null) {
			throw new RuntimeException(
					"No HTableWriteBuffer was passed to the GuiceTableMapper so don't try to write on it!");
		}
		this.writeBuffer.write(put);
	}
}
