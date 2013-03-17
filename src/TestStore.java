import java.io.IOException;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TestStore {
	static Logger logger = Logger.getLogger(TestStore.class.getName());

	public static void main(String[] args) throws IOException {
		String[] pages = { "/", "/a.html", "/b.html", "/c.html" };
		byte[] b = new byte[2048];
		for (int i = 0; i < 2048; i += 8) {
			b[i] = (byte) 0xc;
			b[i + 1] = (byte) 0xa;
			b[i + 2] = (byte) 0xf;
			b[i + 3] = (byte) 0xe;
			b[i + 4] = (byte) 0xb;
			b[i + 5] = (byte) 0xa;
			b[i + 6] = (byte) 0xb;
			b[i + 7] = (byte) 0xe;
		}

		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable htable = new HTable(hbaseConfig, "t1");
		htable.setAutoFlush(false);
		htable.setWriteBufferSize(1024 * 1024 * 12);

		int totalRecords = 100000;
		int maxID = totalRecords / 1000;
		Random rand = new Random();
		logger.finer("importing " + totalRecords + " records ....");
		for (int i = 0; i < totalRecords; i++) {
			int userID = rand.nextInt(maxID) + 1;
			byte[] rowkey = Bytes.add(Bytes.toBytes(userID), Bytes.toBytes(i));
			String randomPage = pages[rand.nextInt(pages.length)];
			Put put = new Put(rowkey);
			put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"), 0l, b);
			htable.put(put);
		}
		htable.flushCommits();
		htable.close();
		logger.finer("done");
	}
}
