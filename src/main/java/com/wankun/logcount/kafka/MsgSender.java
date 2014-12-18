package com.wankun.logcount.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MsgSender extends Thread {
	private final static Logger logger = LoggerFactory.getLogger(MsgSender.class);

	private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");

	private BlockingQueue<String> queue;
	private Producer<String, String> producer;

	public MsgSender(BlockingQueue<String> queue) {
		this.queue = queue;

		Properties props = new Properties();
		props.put("metadata.broker.list", "10.10.102.191:9092,10.10.102.192:9092,10.10.102.193:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		producer = new Producer<String, String>(config);
	}

	@Override
	public void run() {
		while (true) {
			try {
				String line = queue.take();
				if (line != null && !line.replace("\n", "").replace("\r", "").equals("")) {
					String timestamp = sdf.format(new Date());
					KeyedMessage<String, String> data = new KeyedMessage<String, String>("recsys", timestamp, line);
					logger.debug("sending kv :( {}:{})", timestamp, line);
					producer.send(data);
				}
			} catch (InterruptedException e) {
				logger.error("kafka producer 消息发送失败", e);
			}
		}
	}

}