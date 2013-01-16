package ch.unibe.scg.cc.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import ch.unibe.scg.cc.Main;
import ch.unibe.scg.cc.modules.CCModule;
import ch.unibe.scg.cc.modules.HBaseModule;
import ch.unibe.scg.cc.modules.JavaModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class IndexTopHashfacts {
	static Logger logger = Logger.getLogger(IndexTopHashfacts.class);

	public static void main(String[] args) throws IOException {
		ConsoleAppender console = new ConsoleAppender(new PatternLayout());
		logger.addAppender(console);
		logger.setLevel(Level.ALL);
		logger.debug("los");

		Injector i = Guice.createInjector(new CCModule(), new JavaModule(), new HBaseModule());
		HTable indexHashfacts2Functions = i.getInstance(Key.get(HTable.class, Names.named("indexHashfacts2Functions")));
		HTable hashfactContent = i.getInstance(Key.get(HTable.class, Names.named("hashfactContent")));

		Scan scan = new Scan(new byte[] { Main.TYPE_1_CLONE }, new byte[] { Main.TYPE_1_CLONE });
		scan.setCaching(250000);
		scan.addFamily(GuiceResource.FAMILY);
		ResultScanner rsHashfacts = indexHashfacts2Functions.getScanner(scan);
		Iterator<Result> ir = rsHashfacts.iterator();

		TreeMap<Integer, List<byte[]>> hm = new TreeMap<Integer, List<byte[]>>(new Comparator<Integer>() {
			public int compare(Integer a, Integer b) {
				return b.compareTo(a);
			}
		});

		int count = 0;
		while (ir.hasNext()) {
			Result r = ir.next();
			int nf = Bytes.toInt(r.getValue(GuiceResource.FAMILY, GuiceResource.ColumnName.COUNT_FACTS.getName()));
			byte[] rowKey = r.getRow();

			if (hm.get(nf) == null) {
				hm.put(nf, new ArrayList<byte[]>());
			}

			hm.get(nf).add(rowKey);

			count++;
			if (count % 10000 == 0)
				logger.debug(count + "... ");
		}

		int top = 5;
		for (Integer nf : hm.keySet()) {
			logger.debug(nf + ": ");
			for (byte[] v : hm.get(nf)) {
				logger.debug(Arrays.toString(v));
				logger.debug(lookupContent(v, hashfactContent));
				logger.debug("----------------------\n");
			}

			top--;
			if (top < 0)
				break;
		}
	}

	private static String lookupContent(byte[] rowKey, HTable hashfactContent) throws IOException {
		Scan s = new Scan(rowKey);
		Result r = hashfactContent.getScanner(s).iterator().next();
		byte[] res = r.getValue(GuiceResource.FAMILY, Bytes.toBytes("sv"));
		return Bytes.toString(res);
	}
}
