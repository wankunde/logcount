package com.wankun.logcount.kafka;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TailLog extends Thread {
	private final static Logger logger = LoggerFactory.getLogger(TailLog.class);

	private BlockingQueue<String> queue;
	private String logname;
	ByteBuffer buf = ByteBuffer.allocate(4096);

	public TailLog(BlockingQueue<String> queue, String logname) {
		this.queue = queue;
		this.logname = logname;
	}

	@Override
	public void run() {
		RandomAccessFile rfile = null;
		FileChannel ch = null;
		try {
			rfile = new RandomAccessFile(new File(logname), "r");
			ch = rfile.getChannel();

			int pos = 0;
			int limit = 0;
			while (true) {
				if (ch.read(buf) > 0) {
					limit = buf.limit();
					pos = 0;
					buf.flip();
					while (buf.hasRemaining()) {
						byte b = buf.get();
						if (b == '\n') {
							int len = buf.position();
							buf.position(pos);
							buf.limit(len);
							String line = Charset.forName("GBK").decode(buf.slice()).toString();
							logger.debug("new line --> "+line);
							queue.put(line);
							pos = len;
							buf.position(len);
							buf.limit(limit);
						}
					}
					buf.position(pos);
					buf.compact();
				} else {
					Thread.currentThread().sleep(1000);
					// 文件已经切换
					if (ch.position() > rfile.length()) {
						String line = Charset.forName("GBK").decode(buf.slice()).toString();
						queue.put(line);
						buf.clear();

						ch.close();
						rfile.close();
						rfile = new RandomAccessFile(new File(logname), "r");
						ch = rfile.getChannel();
					}
				}
			}
		} catch (Exception e) {
			logger.error("文件读取失败", e);
		} finally {
			if (ch != null)
				try {
					ch.close();
				} catch (IOException e) {
					logger.error("文件 FileChannel ch 关闭失败", e);
				}
			if (rfile != null)
				try {
					rfile.close();
				} catch (IOException e) {
					logger.error("文件 RandomAccessFile rfile 关闭失败", e);
				}
		}
	}
}