package com.wankun.logcount.spark;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

public class RecsysLogs extends HttpServlet {

	private static final long serialVersionUID = 4289573629015709424L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		HTableInterface htable = (HTableInterface) getServletContext().getAttribute("htable");
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("f"), Bytes.toBytes("nums"));
		scan.setReversed(true);
		// scan.setMaxResultSize(20);
		scan.setFilter(new PageFilter(20));
		ResultScanner scanner = htable.getScanner(scan);
		StringBuilder sb = new StringBuilder();
		for (Result res : scanner) {
			Cell cell = res.getColumnLatestCell(Bytes.toBytes("f"), Bytes.toBytes("nums"));
			Long nums = Bytes.toLong(CellUtil.cloneValue(cell));
			String key = Bytes.toString(CellUtil.cloneRow(cell));
			sb.append(key + " : " + nums + "\n");
		}
		scanner.close();

		resp.getWriter().write(sb.toString());
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		this.doGet(req, resp);
	}

}
