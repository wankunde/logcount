package com.wankun.logcount.kafka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TailLogBak extends Thread {
	private final static Logger logger = LoggerFactory.getLogger(TailLogBak.class);

	private BlockingQueue<String> queue;
	private String logname;

	private CharBuffer buf = CharBuffer.allocate(4096);

	public TailLogBak(BlockingQueue<String> queue, String logname) {
		this.queue = queue;
		this.logname = logname;
	}

	@Override
	public void run() {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(new File(logname)));

			long filesize = 0;
			while (true) {
				logger.debug("filesize :{}     current system file size :{} . Log file switchover!", filesize,
						new File(logname).length());
				// 判断文件是否已经切换
				if (filesize > new File(logname).length()) {
					try {
						// 在切换读文件前，读取文件全部内容
						StringBuilder line = new StringBuilder();
						while (reader.read(buf) > 0) {
							buf.flip();
							// 读buffer 并解析
							for (int i = 0; i < buf.limit(); i++) {
								char c = buf.get();
								line.append(c);
								if ((c == '\n') || (c == '\r'))
									if (line.length() > 0) {
										queue.put(line.toString());
										line = new StringBuilder();
									}
							}
						}
						queue.put(line.toString());
						buf.clear();

						// 切换读文件
						if (reader != null)
							reader.close();
						reader = new BufferedReader(new FileReader(new File(logname)));
						filesize = new File(logname).length();
					} catch (Exception e) {
						logger.error("文件 {} 不存在", logname, e);
						Thread.currentThread().sleep(10000);
						continue;
					}
				}

				for (int retrys = 10; retrys > 0; retrys--) {
					int bufread = reader.read(buf);
					if (bufread < 0) {
						if (retrys > 0)
							Thread.currentThread().sleep(1000);
						else {
							// 等待10s后无新数据读出
							buf.flip();
							char[] dst = new char[buf.length()];
							buf.get(dst);
							buf.clear();
							queue.put(new String(dst));
						}
					} else {
						retrys = -1;
						// 文件未切换，更新文件大小size
						long newsize = new File(logname).length();
						if (filesize < newsize) {
							filesize = newsize;
							buf.flip();
							// 读buffer 并解析
							StringBuilder line = new StringBuilder();
							for (int i = 0; i < buf.limit(); i++) {
								char c = buf.get();
								line.append(c);
								if ((c == '\n') || (c == '\r'))
									if (line.length() > 0) {
										queue.put(line.toString());
										line = new StringBuilder();
									}
							}
							// 接着写不完整的数据
							buf.compact();
							if (line.length() > 0)
								buf.append(line);
						}
					}
				}
			}
		} catch (Exception e) {
			logger.error("文件读取失败", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					logger.error("文件 reader 关闭失败", e);
				}
			}
		}

	}

}