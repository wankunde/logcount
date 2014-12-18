package com.wankun.logcount.spark;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.google.common.collect.Maps;

public class LogStream {
	private final static Logger logger = LoggerFactory.getLogger(LogStream.class);

	private static HConnection connection = null;
	private static HTableInterface table = null;
	private static HttpServer2 infoServer = null;

	public static void openHBase(String tablename) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		synchronized (HConnection.class) {
			if (connection == null)
				connection = HConnectionManager.createConnection(conf);
		}

		synchronized (HTableInterface.class) {
			if (table == null) {
				table = connection.getTable("recsys_logs");
			}
		}

		/* start http info server */
		HttpServer2.Builder builder = new HttpServer2.Builder().setName("recsys").setConf(conf);
		InetSocketAddress addr = NetUtils.createSocketAddr("0.0.0.0", 8089);
		builder.addEndpoint(URI.create("http://" + NetUtils.getHostPortString(addr)));
		infoServer = builder.build();

		infoServer.addServlet("monitor", "/monitor", RecsysLogs.class);
		infoServer.setAttribute("htable", table);
		infoServer.setAttribute("conf", conf);
		infoServer.start();
	}

	public static void closeHBase() {
		if (table != null)
			try {
				table.close();
			} catch (IOException e) {
				logger.error("关闭 table 出错", e);
			}
		if (connection != null)
			try {
				connection.close();
			} catch (IOException e) {
				logger.error("关闭 connection 出错", e);
			}
		if (infoServer != null && infoServer.isAlive())
			try {
				infoServer.stop();
			} catch (Exception e) {
				logger.error("关闭 infoServer 出错", e);
			}
	}

	public static void main(String[] args) {
		// open hbase
		try {
			openHBase("logcount");
		} catch (IOException e) {
			logger.error("建立HBase 连接失败", e);
			System.exit(-1);
		}

		SparkConf conf = new SparkConf().setAppName("recsys log stream");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

		Map<String, Integer> topicMap = Maps.newHashMap();
		topicMap.put("recsys", 4);
		JavaPairReceiverInputDStream<String, String> logstream = KafkaUtils.createStream(ssc,
				"10.10.102.191:2181,10.10.102.192:2181,10.10.102.193:2181", "recsys_group1", topicMap);

		JavaDStream<String> lines = logstream.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = -1801798365843350169L;

			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		}).filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 7786877762996470593L;

			@Override
			public Boolean call(String msg) throws Exception {
				return msg.indexOf("character service received paramters:") > 0;
			}
		});

		// 统计Log中的数据，并保存到HBase中
		JavaDStream<Long> nums = lines.count();
		nums.foreachRDD(new Function<JavaRDD<Long>, Void>() {

			private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

			@Override
			public Void call(JavaRDD<Long> rdd) throws Exception {
				Long num = rdd.take(1).get(0);
				String ts = sdf.format(new Date());
				Put put = new Put(Bytes.toBytes(ts));
				put.add(Bytes.toBytes("f"), Bytes.toBytes("nums"), Bytes.toBytes(num));
				table.put(put);
				return null;
			}
		});

		ssc.start();
		ssc.awaitTermination();
	}

}
