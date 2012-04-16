package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/*
 * This is a simple test class in order to introduce the MapReduce and the
 * MapReduce Util class, please keep the comment inside the class
 */

public class WordCountTest {
	static class Mapper1 extends TableMapper<Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);

		public void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException {
			List<KeyValue> list = value.list();
			try {
				for (KeyValue kv : list) {
					// supprime tous sauf 'espace, alpha numeric' + lower case
					for (String w : Bytes.toString(kv.getValue())
							.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase()
							.split(" ")) {
						// System.out.println(w);
						context.write(new Text(w), one);
					}
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	public static class Reducer1 extends
			TableReducer<Text, IntWritable, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			System.out.println("(" + key.toString() + "," + sum + ")");

			/*
			 * int sum = 0; for (IntWritable val : values) { sum += val.get(); }
			 * 
			 * Put put = new Put(key.get()); put.add(Bytes.toBytes("details"),
			 * Bytes.toBytes("total"), Bytes.toBytes(sum));
			 * System.out.println(String
			 * .format("stats :   key : %d,  count : %d",
			 * Bytes.toInt(key.get()), sum)); context.write(key, put);
			 */
		}
	}

	public static void main(String[] args) throws Exception {
		/*
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "opinions");

		ResultScanner scanner = table.getScanner(new Scan());
		ArrayList<String> IdByDate = new ArrayList<String>();

		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
			IdByDate.add(Bytes.toString(rr.getRow()));

		}
		System.out.println(IdByDate);

		for (int ii = 0; ii < IdByDate.size() - 1; ii++) {
			System.out.println(IdByDate.get(ii));
			byte[] start = Bytes.toBytes(IdByDate.get(ii));
			byte[] stop = Bytes.toBytes(IdByDate.get(ii + 1));
			HbaseWrapper.launchMapReduceJob("WordCountTestJob", "opinions",
					new Scan(start, stop), WordCountTest.class, Mapper1.class,
					Reducer1.class, Text.class, IntWritable.class);
		}
		*/
		
		HbaseWrapper.launchMapReduceJob("WordCountTestJob", "opinions",
				new Scan(), WordCountTest.class, Mapper1.class, Reducer1.class,
				Text.class, IntWritable.class);
		
		System.exit(0);
	}
}
